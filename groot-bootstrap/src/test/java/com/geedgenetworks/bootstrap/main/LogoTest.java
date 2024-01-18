package com.geedgenetworks.bootstrap.main;

import com.geedgenetworks.common.Constants;
import org.junit.Assert;
import org.junit.Test;

public class LogoTest {
    @Test
    public void testOutputLogo() {
        Assert.assertTrue(Constants.GROOT_LOGO.length() > 0);
    }
}
