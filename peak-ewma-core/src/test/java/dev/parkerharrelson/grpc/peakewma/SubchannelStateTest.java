package dev.parkerharrelson.grpc.peakewma;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SubchannelStateTest {

    @Test
    void markReady_setsReadySince() {
        SubchannelState st = new SubchannelState();
        assertThat(st.readySinceNanos()).isZero();
        st.markReady(123L);
        assertThat(st.readySinceNanos()).isEqualTo(123L);
    }

    @Test
    void ejectUntil_and_isEjected_behaveAsExpected() {
        SubchannelState st = new SubchannelState();
        long now = 1_000L;
        st.ejectUntil(now + 500L);

        assertThat(st.isEjected(now)).isTrue();
        assertThat(st.isEjected(now + 499L)).isTrue();
        assertThat(st.isEjected(now + 500L)).isFalse();
        assertThat(st.lastEjectEndNanos()).isEqualTo(now + 500L);

        st.ejectUntil(now + 400L);
        assertThat(st.lastEjectEndNanos()).isEqualTo(now + 500L);

        st.ejectUntil(now + 800L);
        assertThat(st.lastEjectEndNanos()).isEqualTo(now + 800L);
    }
}
