package org.apache.brooklyn.cm.salt;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.cm.salt.impl.SaltHighstate;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by geoff on 21/01/2016.
 */
public class HighstateTest {

    private static final Logger LOG = LoggerFactory.getLogger(HighstateTest.class);

    private TestApplication app = null;
    private SaltEntity entity = null;

    @AfterMethod(alwaysRun=true)
    public void tearDown() {
        if ( app != null) {
            Entities.destroyAll(app.getManagementContext());
            app = null;
        }
    }

    @Test
    public void shouldSetSensorsOnEntity() throws Exception {
        String contents = getTestYaml();
        TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        entity = app.createAndManageChild(EntitySpec.create(SaltEntity.class));

        SaltHighstate.applyHighstate(contents, entity);

        final List<String> states = entity.sensors().get(SaltEntity.STATES);
        assertThat(states)
            .contains("apache")
            .contains("apache-reload")
            .contains("apache-restart");

        final Map<String, Object> apachePkgInstalled =
            entity.sensors().get(Sensors.newSensor(SaltHighstate.MODULE_METHOD_TYPE, "apache.pkg.installed", ""));
        assertThat(apachePkgInstalled).isNotNull();
    }


    private String getTestYaml() {
        final ResourceUtils resourceUtils = ResourceUtils.create();
        final InputStream yaml = resourceUtils.getResourceFromUrl("classpath://test-highstate.yaml");
        return Streams.readFullyString(yaml).replaceAll("!!python/unicode", "!!java.lang.String");
    }
}
