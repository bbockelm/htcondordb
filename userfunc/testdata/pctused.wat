;; examplepct -- the documentation's worked scalar UDF: pctUsed(used, total), the
;; percentage of a resource in use.
;;
;; It is the same function docs/user-defined-functions.md shows written in Go, so
;; the two can be read side by side: the rules a UDF follows are the registry's and
;; do not change with the language it is written in.
;;
;; It exports only the SCALAR entry point, which every module must have and which
;; is called once per record. A module wanting the vector entry point as well --
;; where the performance is -- adds it without changing any of this, and
;; docs/wasm-udf-abi.md describes that second convention.
(module
  (memory (export "memory") 1)

  (func (export "classad_udf_abi_version") (result i32) (i32.const 1))

  ;; A tag is numeric when it is boolean (2), integer (3) or real (4). UNDEFINED
  ;; (0), ERROR (1) and string (5) are not, and each is handled by the caller
  ;; rather than coerced here -- undefined and a string mean different things.
  (func $isnum (param $tag i32) (result i32)
    (i32.and (i32.ge_u (local.get $tag) (i32.const 2))
             (i32.le_u (local.get $tag) (i32.const 4))))

  ;; Read a numeric slot as a float. A real's payload is the f64's bits; a boolean
  ;; and an integer share the two's-complement i64 payload.
  (func $num (param $slot i32) (result f64)
    (if (result f64) (i32.eq (i32.load (local.get $slot)) (i32.const 4))
      (then (f64.load offset=8 (local.get $slot)))
      (else (f64.convert_i64_s (i64.load offset=8 (local.get $slot))))))

  ;; Write ERROR into the result slot and report success: the CALL succeeded, and
  ;; its answer is the value ERROR. A nonzero status would instead mean the slot
  ;; was not written at all.
  (func $err (param $res i32)
    (i32.store (local.get $res) (i32.const 1)))

  (func (export "classad_udf_call")
        (param $args i32) (param $argc i32) (param $res i32) (result i32)
    (local $total f64)
    (if (i32.ne (local.get $argc) (i32.const 2))
      (then (return (i32.const 1))))

    ;; The length word is meaningful only for a string, and this function never
    ;; returns one. Clearing it once covers every path below.
    (i32.store offset=4 (local.get $res) (i32.const 0))

    ;; Undefined in, undefined out. An ad that does not publish the attribute has
    ;; not failed, and answering ERROR would make a Requirements expression reject
    ;; the machine rather than ignore the term.
    (if (i32.or (i32.eqz (i32.load (local.get $args)))
                (i32.eqz (i32.load offset=16 (local.get $args))))
      (then
        (i32.store (local.get $res) (i32.const 0))
        (return (i32.const 0))))

    ;; Anything else that is not a number -- a string, an ERROR arriving from the
    ;; caller's own arithmetic -- is ERROR.
    (if (i32.eqz (i32.and
          (call $isnum (i32.load (local.get $args)))
          (call $isnum (i32.load offset=16 (local.get $args)))))
      (then
        (call $err (local.get $res))
        (return (i32.const 0))))

    (local.set $total (call $num (i32.add (local.get $args) (i32.const 16))))
    (if (f64.eq (local.get $total) (f64.const 0))
      (then
        (call $err (local.get $res))
        (return (i32.const 0))))

    (i32.store (local.get $res) (i32.const 4))
    (f64.store offset=8 (local.get $res)
      (f64.mul (f64.div (call $num (local.get $args)) (local.get $total))
               (f64.const 100)))
    (i32.const 0))
)
