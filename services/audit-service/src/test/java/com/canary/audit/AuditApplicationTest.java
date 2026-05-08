package com.canary.audit;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class AuditApplicationTest {

    @Test
    void contextLoads() {
        // Spring context starts cleanly with both gating flags off.
    }
}
