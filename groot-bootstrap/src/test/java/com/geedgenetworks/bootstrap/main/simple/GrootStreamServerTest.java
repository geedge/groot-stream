package com.geedgenetworks.bootstrap.main.simple;

import com.geedgenetworks.bootstrap.command.Command;
import com.geedgenetworks.bootstrap.command.CommandArgs;
import com.geedgenetworks.bootstrap.command.ExecuteCommandArgs;
import com.geedgenetworks.bootstrap.enums.EngineType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.utils.CommandLineUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
public class GrootStreamServerTest {
    public static void main(String[] args) {
        ExecuteCommandArgs bootstrapCommandArgs = CommandLineUtils
                .parse(args, new ExecuteCommandArgs(), EngineType.FLINK13.getShellName(), true);
        run(bootstrapCommandArgs.buildCommand());
    }

    public static <T extends CommandArgs> void run(Command<T> command) throws JobExecuteException {
        try {
            command.execute();
        } catch (ConfigCheckException e) {
            outputConfigError(e);
            throw e;
        } catch (Exception e) {
            outputFatalError(e);
            throw e;
        }
    }
    private static void outputConfigError(Throwable throwable) {
        log.error(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        log.error("Config Error:\n");
        log.error("Reason: {} \n", errorMsg);
        log.error(
                "\n===============================================================================\n\n\n");
    }


    private static void outputFatalError(Throwable throwable) {
        log.error("\\n\\n===============================================================================\\n\\n");
        String errorMsg = throwable.getMessage();
        log.error("Fatal Error ,Reason is :{} \n", errorMsg);
        log.error("Exception StackTrace :{}", ExceptionUtils.getStackTrace(throwable));
        log.error("\\n\\n===============================================================================\\n\\n");
    }




}
