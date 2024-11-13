package com.geedgenetworks.bootstrap.command;

import com.beust.jcommander.Parameter;
import com.geedgenetworks.bootstrap.enums.DeployMode;
import com.geedgenetworks.common.config.Constants;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.List;

@EqualsAndHashCode
@Data
public abstract class CommandArgs {
    @Parameter(names = {"-h", "--help", "help"},
            help = true,
            description = "help message")
    protected boolean help = false;

    @Parameter(names = {"-v", "--version", "version"},
            description = "current version")
    protected boolean version = false;

    @Parameter(names = {"-c", "--config"},
            required = true,
            description = "config file")
    protected String configFile;

    @Parameter(
            names = {"--check"},
            description = "Whether check config")
    protected boolean checkConfig = false;

    // user-defined parameters
    @Parameter(names = {"-i", "--variable"},
              splitter = ParameterSplitter.class,
              description = "Job level user-defined parameters, such as -i traffic_file_bucket=traffic_file_bucket, or -i scheduler.knowledge_base.update.interval.minutes=1. "
                      + "We use ',' as separator, when inside \"\", ',' are treated as normal characters instead of delimiters.")
    protected List<String> variables = Collections.emptyList();

    @Parameter(names = {"-n", "--name"},
              description = "job name")
    protected String jobName = Constants.DEFAULT_JOB_NAME;

    @Parameter(names = {"--encrypt"},
               description = "Encrypt config file, when both --decrypt and --encrypt are specified, only --encrypt will take effect")
    protected boolean encrypt = false;

    @Parameter(names = {"--decrypt"},
              description = "Decrypt config file, When both --decrypt and --encrypt are specified, only --encrypt will take effect")
    protected boolean decrypt = false;

    protected List<String> originalParameters = Collections.emptyList();

    public  abstract Command buildCommand();

    public abstract DeployMode getDeployMode();



}
