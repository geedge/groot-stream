package com.geedgenetworks.bootstrap.main;

import com.geedgenetworks.bootstrap.command.Command;
import com.geedgenetworks.bootstrap.command.CommandArgs;
import com.geedgenetworks.bootstrap.command.ExecuteCommandArgs;
import com.geedgenetworks.bootstrap.enums.EngineType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.utils.CommandLineUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
public class GrootStreamServer {
    public static void main(String[] args) {
        ExecuteCommandArgs bootstrapCommandArgs = CommandLineUtils
                .parse(args, new ExecuteCommandArgs(), EngineType.FLINK13.getShellName(), true);
        run(bootstrapCommandArgs.buildCommand());
    }

    public static <T extends CommandArgs> void run(Command<T> command) throws JobExecuteException {
        try {
            command.execute();
        } catch (Exception e) {
            outputFatalError(e);
            throw  e;
        }
    }

    private static void outputFatalError(Throwable throwable) {
        log.error("\\n\\n===============================================================================\\n\\n");
        log.error("Fatal Error ,Reason is :{} \n", throwable.getMessage());
        log.error("Exception StackTrace :{}", ExceptionUtils.getStackTrace(throwable));
        log.error("\\n\\n===============================================================================\\n\\n");
    }




}
