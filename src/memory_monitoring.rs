use std::fs;
use std::path::PathBuf;

/// Get peak RSS (Resident Set Size) - peak physical memory in KB
pub fn get_peak_memory_kb() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if line.starts_with("VmHWM:") {
            // VmHWM = High Water Mark for RSS (peak physical memory)
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                return parts[1].parse::<u64>().ok(); // value is in kB
            }
        }
    }
    None
}

/// Get peak memory from cgroup (if available) - this is more accurate for cgroup-limited processes
/// Returns peak memory in KB
pub fn get_peak_memory_from_cgroup_kb() -> Option<u64> {
    // Try to read memory.peak from cgroup v2 (in bytes)
    let peak_bytes = read_memory_file("memory.peak")?;
    // Convert bytes to KB
    Some(peak_bytes / 1024)
}


pub fn get_cgroup_path() -> Option<PathBuf> {
    let data = fs::read_to_string("/proc/self/cgroup").ok()?;
    for line in data.lines() {
        let parts: Vec<&str> = line.splitn(3, ':').collect();
        if parts.len() == 3 {
            // For cgroup v2, format is: 0::/path/to/cgroup
            // parts[0] = hierarchy ID (0 for v2)
            // parts[1] = controller list (empty for v2)
            // parts[2] = cgroup path
            let cgroup_path = parts[2].trim_start_matches('/');
            let mut path = PathBuf::from("/sys/fs/cgroup");
            path.push(cgroup_path);
            
            // Verify the path exists and has memory.max
            if path.join("memory.max").exists() {
                return Some(path);
            }
        }
    }
    None
}

pub fn read_memory_file(file_name: &str) -> Option<u64> {
    let path = get_cgroup_path()?.join(file_name);
    let data = fs::read_to_string(path).ok()?;
    let trimmed = data.trim();
    if trimmed == "max" {
        None // no limit set
    } else {
        trimmed.parse::<u64>().ok()
    }
}

pub fn get_memory_max() -> usize {
    let mem = read_memory_file("memory.max");
    // cgroup v2 values are in bytes, convert to KB
    let max_memory = ( mem.unwrap_or(1000000000000) as f64 / 1024.0) as usize;
    max_memory
}

pub fn get_available_memory_kb() -> usize {
    // Get current memory usage and max limit
    let current_usage_bytes = read_memory_file("memory.current").unwrap_or(0);
    let max_limit_bytes = read_memory_file("memory.max").unwrap_or(1000000000000);
    
    // Calculate available memory: max - current (bytes) and convert to KB
    let available_bytes = max_limit_bytes.saturating_sub(current_usage_bytes);
    let available_memory_kb = (available_bytes as f64 / 1024.0) as usize;
    
    available_memory_kb
}

/// Force memory reclamation by temporarily setting memory.high below memory.current
/// This triggers the kernel's memory reclaimer to free up "ghost memory" from killed processes
/// This is an async function that should be called from async context to avoid blocking
pub async fn trigger_memory_reclamation() -> bool {
    let cgroup_path = match get_cgroup_path() {
        Some(path) => path,
        None => return false,
    };
    
    let memory_high_path = cgroup_path.join("memory.high");
    let memory_current_path = cgroup_path.join("memory.current");
    
    // Read current memory usage
    let current_bytes = match fs::read_to_string(&memory_current_path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
    {
        Some(val) => val,
        None => return false,
    };
    
    // Don't trigger if current usage is very low (nothing to reclaim)
    if current_bytes < 100 * 1024 * 1024 { // Less than 100MB
        return false;
    }
    
    // Read current memory.high (if set)
    let original_high = fs::read_to_string(&memory_high_path)
        .ok()
        .and_then(|s| {
            let trimmed = s.trim();
            if trimmed == "max" {
                None
            } else {
                trimmed.parse::<u64>().ok()
            }
        });
    
    // Set memory.high to 95% of current usage (less aggressive to avoid throttling)
    // This creates memory pressure that forces the kernel to reclaim memory
    let target_high_bytes = (current_bytes * 95) / 100;
    
    // Write the new memory.high value
    if fs::write(&memory_high_path, target_high_bytes.to_string()).is_err() {
        return false;
    }
    
    // Wait for kernel to process the reclamation (use async sleep to avoid blocking)
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    
    // Restore original memory.high (or "max" if it wasn't set) immediately
    // Don't wait too long with memory.high set low to avoid throttling
    let restore_value = match original_high {
        Some(val) => val.to_string(),
        None => "max".to_string(),
    };
    let _ = fs::write(&memory_high_path, restore_value);
    
    // Additional wait to allow reclamation to complete (after restoring)
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    
    true
}
