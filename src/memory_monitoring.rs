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
