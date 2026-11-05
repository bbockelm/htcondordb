;; examplemean -- the documentation's worked aggregate: the arithmetic mean of an
;; attribute, over whatever rows a group holds.
;;
;; It is the smallest module that exercises all four aggregate operations and the
;; optional numeric fold, and it is a mean rather than a sum because a mean cannot
;; be reconstructed from another group's answer: a merge that forgot the count
;; would be WRONG rather than merely incomplete, and that is the mistake the laws
;; are there to catch.
;;
;; The state is 16 bytes the HOST holds, not a handle into this module's memory:
;;
;;   0..7    f64  the sum of the values folded so far
;;   8..15   i64  how many values that was
;;
;; init, fold and merge return the LENGTH of the state they wrote, or a negative
;; number to refuse. result returns a status, like a scalar call.
(module
  (memory (export "memory") 1)

  (func (export "classad_udaf_abi_version") (result i32) (i32.const 1))

  ;; The identity: the answer for a group that contributed no rows at all. Zero
  ;; values, not zero mean -- which is why result answers UNDEFINED for it rather
  ;; than 0.
  (func $init (param $out i32) (param $cap i32) (result i32)
    (if (i32.lt_u (local.get $cap) (i32.const 16))
      (then (return (i32.const -1))))
    (memory.fill (local.get $out) (i32.const 0) (i32.const 16))
    (i32.const 16))

  ;; Fold a batch of value slots into a state. A BATCH, not a value: the state is
  ;; copied in and out across the host boundary on every call, so folding many
  ;; values per call is what makes that copy affordable.
  (func $fold (param $sp i32) (param $sl i32) (param $vp i32) (param $n i32)
              (param $op i32) (param $oc i32) (result i32)
    (local $i i32) (local $slot i32) (local $tag i32)
    (local $sum f64) (local $cnt i64)
    (if (i32.ne (local.get $sl) (i32.const 16))
      (then (return (i32.const -1))))
    (if (i32.lt_u (local.get $oc) (i32.const 16))
      (then (return (i32.const -1))))
    (local.set $sum (f64.load (local.get $sp)))
    (local.set $cnt (i64.load offset=8 (local.get $sp)))
    (block $done
      (loop $next
        (br_if $done (i32.ge_u (local.get $i) (local.get $n)))
        (local.set $slot
          (i32.add (local.get $vp) (i32.mul (local.get $i) (i32.const 16))))
        (local.set $tag (i32.load (local.get $slot)))
        (if (i32.eq (local.get $tag) (i32.const 4))
          (then
            (local.set $sum
              (f64.add (local.get $sum) (f64.load offset=8 (local.get $slot))))
            (local.set $cnt (i64.add (local.get $cnt) (i64.const 1))))
          (else
            ;; A boolean (2) or an integer (3) shares the i64 payload. Everything
            ;; else -- UNDEFINED, ERROR, a string -- is SKIPPED, which is what the
            ;; built-in numeric aggregates do and what lets this module make the
            ;; promise below about the column pass.
            (if (i32.or (i32.eq (local.get $tag) (i32.const 2))
                        (i32.eq (local.get $tag) (i32.const 3)))
              (then
                (local.set $sum
                  (f64.add (local.get $sum)
                           (f64.convert_i64_s (i64.load offset=8 (local.get $slot)))))
                (local.set $cnt (i64.add (local.get $cnt) (i64.const 1)))))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $next)))
    (f64.store (local.get $op) (local.get $sum))
    (i64.store offset=8 (local.get $op) (local.get $cnt))
    (i32.const 16))

  ;; Combine two partial states. This must be ASSOCIATIVE AND COMMUTATIVE: the
  ;; engine fans the pass out across segment windows and which worker draws which
  ;; segment is a race, so a merge that depended on the order would be right in
  ;; every test its author wrote and wrong in production. Addition is both.
  (func $merge (param $ap i32) (param $al i32) (param $bp i32) (param $bl i32)
               (param $op i32) (param $oc i32) (result i32)
    (if (i32.or (i32.ne (local.get $al) (i32.const 16))
                (i32.ne (local.get $bl) (i32.const 16)))
      (then (return (i32.const -1))))
    (if (i32.lt_u (local.get $oc) (i32.const 16))
      (then (return (i32.const -1))))
    (f64.store (local.get $op)
      (f64.add (f64.load (local.get $ap)) (f64.load (local.get $bp))))
    (i64.store offset=8 (local.get $op)
      (i64.add (i64.load offset=8 (local.get $ap)) (i64.load offset=8 (local.get $bp))))
    (i32.const 16))

  ;; Render the state as one value slot. UNDEFINED for a group that held no
  ;; numbers: there is no mean of nothing, and answering 0 would invent one.
  (func $result (param $sp i32) (param $sl i32) (param $rp i32) (result i32)
    (local $cnt i64)
    (if (i32.ne (local.get $sl) (i32.const 16))
      (then (return (i32.const 1))))
    (i32.store offset=4 (local.get $rp) (i32.const 0))
    (local.set $cnt (i64.load offset=8 (local.get $sp)))
    (if (i64.eqz (local.get $cnt))
      (then
        (i32.store (local.get $rp) (i32.const 0))
        (return (i32.const 0))))
    (i32.store (local.get $rp) (i32.const 4))
    (f64.store offset=8 (local.get $rp)
      (f64.div (f64.load (local.get $sp)) (f64.convert_i64_s (local.get $cnt))))
    (i32.const 0))

  (export "classad_udaf_init" (func $init))
  (export "classad_udaf_fold" (func $fold))
  ;; Exporting fold under this second name DECLARES two things the host cannot
  ;; check: that the answer does not depend on the order values arrive in, and
  ;; that the module does not count what it was never offered. Both hold here --
  ;; addition commutes, and a value the column pass withholds is one this fold
  ;; would have skipped. Without it the aggregate is served from the record scan,
  ;; which is correct and slower, never wrong.
  (export "classad_udaf_fold_num" (func $fold))
  (export "classad_udaf_merge" (func $merge))
  (export "classad_udaf_result" (func $result))
)
