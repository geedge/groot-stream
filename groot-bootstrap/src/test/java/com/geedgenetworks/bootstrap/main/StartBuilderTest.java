package com.geedgenetworks.bootstrap.main;

import com.geedgenetworks.bootstrap.utils.StartBuilder;
import com.geedgenetworks.common.utils.FileUtils;
import lombok.NonNull;
import org.junit.jupiter.api.Test;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StartBuilderTest {
    static {
        StartBuilder.setStarter(true);

    }

    @Test
    public void testGetPluginsJarDependencies() throws Exception {
        String rootPath = "/tmp/groot-stream";
        StartBuilder.setGrootStreamHome(rootPath);
        String pluginPath = rootPath + "/plugins";
        String jarFile1 = pluginPath + File.separator
                + "connector-1" + File.separator + "lib" + File.separator + "plugin1.jar";
        String jarFile2 = pluginPath + File.separator
                + "connector-2" + File.separator + "lib" + File.separator + "plugin2.jar";
        FileUtils.createNewFile(jarFile1);
        FileUtils.createNewFile(jarFile2);
        writeTestDataToFile(jarFile1);
        writeTestDataToFile(jarFile2);
        assertEquals(StartBuilder.getPluginsJarDependencies().size(),2);

    }

    public void writeTestDataToFile(@NonNull String filePath) throws IOException {
        FileUtils.createNewFile(filePath);

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filePath))) {
            for (int i = 0; i < 100; i++) {
                bw.write(i + "");
                bw.newLine();
            }
        }
    }


}
