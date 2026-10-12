//! BN254 field and G1 curve checks on `U256`, used in place of `ark-bn254` to add no dependency.

use alloy::primitives::{U256, uint};

/// Base field modulus `q` (= `ark-bn254` `Fq`): G1 point coordinates live in `F_q`.
pub(super) const Q: U256 =
    uint!(0x30644e72e131a029b85045b68181585d97816a916871ca8d3c208c16d87cfd47_U256);
/// Scalar field modulus `r` (= `ark-bn254` `Fr`), the G1 order: proof scalars live in `F_r`.
pub(super) const R: U256 =
    uint!(0x30644e72e131a029b85045b68181585d2833e84879b9709143e1f593f0000001_U256);
/// `b` in the G1 curve equation `y² = x³ + b`.
const B: U256 = uint!(3_U256);

/// Returns whether `v` is a canonical base field element (`v < q`).
pub(super) fn is_base_field_element(v: U256) -> bool {
    v < Q
}

/// Returns whether `v` is a canonical scalar field element (`v < r`).
pub(super) fn is_scalar_field_element(v: U256) -> bool {
    v < R
}

/// Returns whether `(x, y)` satisfies `y² = x³ + 3 (mod q)`.
///
/// `(0, 0)` fails: unlike `ark-bn254`, L1 has no encoding for the point at infinity.
pub(super) fn is_on_curve(x: U256, y: U256) -> bool {
    let lhs = y.mul_mod(y, Q);
    let rhs = x.mul_mod(x, Q).mul_mod(x, Q).add_mod(B, Q);
    lhs == rhs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_is_on_curve() {
        assert!(is_on_curve(uint!(1_U256), uint!(2_U256)));
    }

    #[test]
    fn negated_generator_is_on_curve() {
        assert!(is_on_curve(uint!(1_U256), Q - uint!(2_U256)));
    }

    #[test]
    fn zero_point_is_not_on_curve() {
        assert!(!is_on_curve(U256::ZERO, U256::ZERO));
    }

    #[test]
    fn moduli_are_not_field_elements() {
        assert!(!is_base_field_element(Q));
        assert!(!is_scalar_field_element(R));
        assert!(is_base_field_element(Q - uint!(1_U256)));
        assert!(is_scalar_field_element(R - uint!(1_U256)));
    }
}
