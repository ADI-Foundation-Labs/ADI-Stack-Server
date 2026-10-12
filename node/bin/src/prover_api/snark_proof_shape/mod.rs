//! Structural check of a SNARK proof against what the L1 PLONK verifier's `loadProof` accepts.

mod bn254;

use std::ops::Range;

use alloy::primitives::U256;

const WORD_BYTES: usize = 32;
/// Proof length of the Airbender PLONK wrapper with a non-recursive VK.
const PROOF_WORDS: usize = 44;
const PROOF_BYTES: usize = PROOF_WORDS * WORD_BYTES;
/// Words holding G1 points as consecutive `(x, y)` pairs.
const POINT_WORDS: [Range<usize>; 2] = [0..22, 40..44];
/// Words holding Fr scalars.
const SCALAR_WORDS: Range<usize> = 22..40;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ProofShapeError {
    #[error("proof has {0} bytes, expected {expected}", expected = PROOF_BYTES)]
    Length(usize),
    #[error("word {0} is not below the field modulus")]
    NotCanonical(usize),
    #[error("point at word {0} is not on the BN254 curve")]
    NotOnCurve(usize),
}

/// Rejects a proof that L1 `loadProof` would revert on.
///
/// Stricter than L1: L1 reduces out-of-range words instead of rejecting them, but an honest
/// prover never emits them. A well-formed but wrong proof still passes.
pub(crate) fn check_snark_proof_shape(proof: &[u8]) -> Result<(), ProofShapeError> {
    if proof.len() != PROOF_BYTES {
        return Err(ProofShapeError::Length(proof.len()));
    }
    let words: Vec<U256> = proof
        .chunks_exact(WORD_BYTES)
        .map(U256::from_be_slice)
        .collect();

    POINT_WORDS
        .into_iter()
        .flat_map(|range| range.step_by(2))
        .try_for_each(|i| check_point(&words, i))?;
    SCALAR_WORDS
        .clone()
        .try_for_each(|i| check_scalar(&words, i))
}

/// Checks the G1 point at words `i`, `i + 1`.
fn check_point(words: &[U256], i: usize) -> Result<(), ProofShapeError> {
    let (x, y) = (words[i], words[i + 1]);
    if !bn254::is_base_field_element(x) {
        return Err(ProofShapeError::NotCanonical(i));
    }
    if !bn254::is_base_field_element(y) {
        return Err(ProofShapeError::NotCanonical(i + 1));
    }
    if !bn254::is_on_curve(x, y) {
        return Err(ProofShapeError::NotOnCurve(i));
    }
    Ok(())
}

fn check_scalar(words: &[U256], i: usize) -> Result<(), ProofShapeError> {
    if !bn254::is_scalar_field_element(words[i]) {
        return Err(ProofShapeError::NotCanonical(i));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::uint;

    use super::{
        bn254::{Q, R},
        *,
    };

    /// G1 generator `(1, 2)`.
    const G: (U256, U256) = (uint!(1_U256), uint!(2_U256));

    /// A real proof from era-contracts v0.30.0 `PlonkVerifier.t.sol` `setUp()`.
    const REAL_PROOF: [U256; PROOF_WORDS] = [
        uint!(10032255692304426541958487424837706541667730769782503366592797609781788557424_U256),
        uint!(11856023086316274558845067687080284266010851703055534566998849536424959073766_U256),
        uint!(1946976494418613232642071265529572704802622739887191787991738703483400525159_U256),
        uint!(1328106069458824013351862477593422369726189688844441245167676630500797673929_U256),
        uint!(15488976127650523079605218040232167291115155239002840072043251018873550258833_U256),
        uint!(4352460820258659596860226525221943504756149602617718032378962471842121872064_U256),
        uint!(10499239305859992443759785453270906003243074359959242371675950941500942473773_U256),
        uint!(21347231097799123231227724221565041889687686131480556177475242020711996173235_U256),
        uint!(21448274562455512652922184359722637546669181231038098300951155169465175447933_U256),
        uint!(5224615512030263722410009061780530125927659699046094954022444377569738464640_U256),
        uint!(457781538876079938778845275495204146302569607395268192839148474821758081582_U256),
        uint!(18861735728246155975127314860333796285284072325207684293054713266899263027595_U256),
        uint!(16303944945368742900183889655415585360236645961122617249176044814801835577336_U256),
        uint!(13035945439947210396602249585896632733250124877036427100939804737514358838409_U256),
        uint!(5344210729159253547334947774998425118220137275601995670629358314205854915831_U256),
        uint!(5798533246034358556434877465898581616792677631188370022078168611592512620805_U256),
        uint!(17389657286129893116489015409587246992530648956814855147744210777822507444908_U256),
        uint!(2287244647342394712608648573347732257083870498255199596324312699868511383792_U256),
        uint!(4008043766112513713076111464601725311991199944328610186851424132679188418647_U256),
        uint!(1192776719848445147414966176395169615865534126881763324071908049917030138759_U256),
        uint!(21297794452895123333253856666749932934399762330444876027734824957603009458926_U256),
        uint!(17125994169200693606182326100834606153690416627082476471630567824088261322122_U256),
        uint!(13696978282153979214307382954559709118587582183649354744253374201589715565327_U256),
        uint!(19885518441500677676836488338931187143852666523909650686513498826535451677070_U256),
        uint!(1205434280320863211046275554464591162919269140938371417889032165323835178587_U256),
        uint!(17633172995805911347980792921300006225132501482343225088847242025756974009163_U256),
        uint!(16438080406761371143473961144300947125022788905488819913014533292593141026205_U256),
        uint!(5069081552536259237104332491140391551180511112980430307676595350165020188468_U256),
        uint!(21217317205917200275887696442048162383709998732382676029165079037795626916156_U256),
        uint!(19474466610515117278975027596198570980840609656738255347763182823792179771539_U256),
        uint!(9744176601826774967534277982058590459006781888895542911226406188087317156914_U256),
        uint!(13171230402193025939763214267878900142876558410430734782028402821166810894141_U256),
        uint!(11775403006142607980192261369108550982244126464568678337528680604943636677964_U256),
        uint!(6903612341636669639883555213872265187697278660090786759295896380793937349335_U256),
        uint!(10197105415769290664169006387603164525075746474380469980600306405504981186043_U256),
        uint!(10143152486514437388737642096964118742712576889537781270260677795662183637771_U256),
        uint!(7662095231333811948165764727904932118187491073896301295018543320499906824310_U256),
        uint!(929422796511992741418500336817719055655694499787310043166783539202506987065_U256),
        uint!(13837024938095280064325737989251964639823205065380219552242839155123572433059_U256),
        uint!(11738888513780631372636453609299803548810759208935038785934252961078387526204_U256),
        uint!(16528875312985292109940444015943812939751717229020635856725059316776921546668_U256),
        uint!(17525167117689648878398809303253004706004801107861280044640132822626802938868_U256),
        uint!(7419167499813234488108910149511390953153207250610705609008080038658070088540_U256),
        uint!(11628425014048216611195735618191126626331446742771562481735017471681943914146_U256),
    ];

    fn proof_from_words(words: &[U256]) -> Vec<u8> {
        words
            .iter()
            .flat_map(|word| word.to_be_bytes::<WORD_BYTES>())
            .collect()
    }

    /// Builds 11 × `G`, 18 × `r - 1`, 2 × `-G`, so values just below each modulus are covered.
    fn valid_words() -> Vec<U256> {
        let mut words = [G.0, G.1].repeat(11);
        words.extend([R - uint!(1_U256); 18]);
        words.extend([G.0, Q - G.1].repeat(2));
        words
    }

    #[test]
    fn accepts_well_formed_proof() {
        let proof = proof_from_words(&valid_words());

        assert_eq!(check_snark_proof_shape(&proof), Ok(()));
    }

    #[test]
    fn accepts_real_plonk_proof() {
        assert_eq!(
            check_snark_proof_shape(&proof_from_words(&REAL_PROOF)),
            Ok(())
        );
    }

    #[test]
    fn rejects_length_not_multiple_of_word() {
        assert_eq!(
            check_snark_proof_shape(&[0; PROOF_BYTES - 1]),
            Err(ProofShapeError::Length(PROOF_BYTES - 1))
        );
    }

    #[test]
    fn rejects_64_zero_bytes() {
        assert_eq!(
            check_snark_proof_shape(&[0; 64]),
            Err(ProofShapeError::Length(64))
        );
    }

    #[test]
    fn rejects_proof_one_word_short() {
        let proof = proof_from_words(&valid_words()[..PROOF_WORDS - 1]);

        assert_eq!(
            check_snark_proof_shape(&proof),
            Err(ProofShapeError::Length(PROOF_BYTES - WORD_BYTES))
        );
    }

    #[test]
    fn rejects_44_zero_words() {
        assert_eq!(
            check_snark_proof_shape(&[0; PROOF_BYTES]),
            Err(ProofShapeError::NotOnCurve(0))
        );
    }

    #[test]
    fn rejects_zero_point() {
        let mut words = valid_words();
        words[40] = U256::ZERO;
        words[41] = U256::ZERO;

        assert_eq!(
            check_snark_proof_shape(&proof_from_words(&words)),
            Err(ProofShapeError::NotOnCurve(40))
        );
    }

    #[test]
    fn rejects_point_off_curve() {
        let mut words = valid_words();
        words[3] = uint!(3_U256);

        assert_eq!(
            check_snark_proof_shape(&proof_from_words(&words)),
            Err(ProofShapeError::NotOnCurve(2))
        );
    }

    #[test]
    fn rejects_unreduced_coordinate() {
        // `q + 1 ≡ 1 (mod q)`, so L1 would read this point as `G` and accept it.
        let mut words = valid_words();
        words[0] = Q + G.0;

        assert_eq!(
            check_snark_proof_shape(&proof_from_words(&words)),
            Err(ProofShapeError::NotCanonical(0))
        );
    }

    #[test]
    fn rejects_scalar_equal_to_r() {
        let mut words = valid_words();
        words[22] = R;

        assert_eq!(
            check_snark_proof_shape(&proof_from_words(&words)),
            Err(ProofShapeError::NotCanonical(22))
        );
    }

    #[test]
    fn rejects_unreduced_y() {
        let mut words = valid_words();
        words[1] = Q + G.1;

        assert_eq!(
            check_snark_proof_shape(&proof_from_words(&words)),
            Err(ProofShapeError::NotCanonical(1))
        );
    }

    #[test]
    fn rejects_last_point_off_curve() {
        let mut words = valid_words();
        words[43] = uint!(3_U256);

        assert_eq!(
            check_snark_proof_shape(&proof_from_words(&words)),
            Err(ProofShapeError::NotOnCurve(42))
        );
    }
}
