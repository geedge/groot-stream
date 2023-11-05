package com.geedgenetworks.bootstrap.command;

import com.geedgenetworks.bootstrap.exception.CommandExecuteException;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;

@FunctionalInterface
public interface Command <T extends CommandArgs> {

    void execute() throws CommandExecuteException, ConfigCheckException;
}

