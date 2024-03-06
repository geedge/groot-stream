package com.geedgenetworks.bootstrap.command;

import com.geedgenetworks.bootstrap.exception.CommandExecuteException;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

public class ConfVersionCommand implements Command<ExecuteCommandArgs> {
    private final ExecuteCommandArgs executeCommandArgs;
    public ConfVersionCommand(ExecuteCommandArgs executeCommandArgs) {
        this.executeCommandArgs = executeCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        try (InputStream input = ConfVersionCommand.class.getClassLoader().getResourceAsStream("META-INF/maven/com.geedgenetworks/groot-bootstrap/pom.properties")) {
            if (input == null) {
                System.out.println("pom.properties file not found.");
                return;
            }
            Properties prop = new Properties();
            prop.load(input);
            String version = prop.getProperty("version");
            System.out.println("Current version: " + version);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
