package com.geedgenetworks.bootstrap.command;

import com.geedgenetworks.bootstrap.exception.CommandExecuteException;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.utils.ConfigFileUtils;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

@Slf4j
public class ConfValidateCommand implements Command<ExecuteCommandArgs> {
    private final ExecuteCommandArgs executeCommandArgs;

    public ConfValidateCommand(ExecuteCommandArgs executeCommandArgs) {
        this.executeCommandArgs = executeCommandArgs;
    }


    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        Path configFile = ConfigFileUtils.getConfigPath(executeCommandArgs);
        // todo validate config file
    }
}
