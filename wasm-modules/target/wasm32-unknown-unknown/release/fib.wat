(module $fib.wasm
  (type (;0;) (func (param i64) (result i64)))
  (func $fib (type 0) (param i64) (result i64)
    (local i64 i64 i64)
    block  ;; label = @1
      local.get 0
      i64.eqz
      i32.eqz
      br_if 0 (;@1;)
      i64.const 1
      return
    end
    local.get 0
    i64.const 7
    i64.and
    local.set 1
    block  ;; label = @1
      block  ;; label = @2
        local.get 0
        i64.const 8
        i64.ge_u
        br_if 0 (;@2;)
        i64.const 1
        local.set 2
        i64.const 1
        local.set 0
        br 1 (;@1;)
      end
      local.get 0
      i64.const -8
      i64.and
      local.set 3
      i64.const 1
      local.set 2
      i64.const 1
      local.set 0
      loop  ;; label = @2
        local.get 0
        local.get 2
        i64.add
        local.tee 2
        local.get 0
        i64.add
        local.tee 0
        local.get 2
        i64.add
        local.tee 2
        local.get 0
        i64.add
        local.tee 0
        local.get 2
        i64.add
        local.tee 2
        local.get 0
        i64.add
        local.tee 0
        local.get 2
        i64.add
        local.tee 2
        local.get 0
        i64.add
        local.set 0
        local.get 3
        i64.const -8
        i64.add
        local.tee 3
        i64.eqz
        i32.eqz
        br_if 0 (;@2;)
      end
    end
    block  ;; label = @1
      local.get 1
      i64.eqz
      br_if 0 (;@1;)
      local.get 0
      local.set 3
      loop  ;; label = @2
        local.get 3
        local.get 2
        i64.add
        local.set 0
        local.get 3
        local.set 2
        local.get 0
        local.set 3
        local.get 1
        i64.const -1
        i64.add
        local.tee 1
        i64.const 0
        i64.ne
        br_if 0 (;@2;)
      end
    end
    local.get 0)
  (table (;0;) 1 1 funcref)
  (memory (;0;) 16)
  (global $__stack_pointer (mut i32) (i32.const 1048576))
  (global (;1;) i32 (i32.const 1048576))
  (global (;2;) i32 (i32.const 1048576))
  (export "memory" (memory 0))
  (export "fib" (func $fib))
  (export "__data_end" (global 1))
  (export "__heap_base" (global 2)))
