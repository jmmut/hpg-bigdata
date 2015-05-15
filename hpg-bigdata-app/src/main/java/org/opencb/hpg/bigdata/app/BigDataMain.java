/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.hpg.bigdata.app;

import org.apache.log4j.Logger;
import org.opencb.hpg.bigdata.app.cli.AlignCommandExecutor;
import org.opencb.hpg.bigdata.app.cli.BamCommandExecutor;
import org.opencb.hpg.bigdata.app.cli.CliOptionsParser;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.app.cli.FastqCommandExecutor;
import org.opencb.hpg.bigdata.app.cli.Ga4ghCommandExecutor;

import com.beust.jcommander.ParameterException;
import org.opencb.hpg.bigdata.core.spark.Wordcount;

/**
 * Created by imedina on 15/03/15.
 */
public class BigDataMain {
    protected static Logger logger = Logger.getLogger(BigDataMain.class);

    public static void main(String[] args) {
        Wordcount wordcount = new Wordcount();
        
        if (args.length == 2) {
            wordcount.sparkAvro(args[0], args[1]);
        } else {
            System.out.println("wrong args, I need 2: file path, pattern to match");
            for (String arg : args) {
                System.out.println("arg = " + arg);
            }
        }
    }
    
    public static void main2(String[] args) {
        CliOptionsParser cliOptionsParser = new CliOptionsParser();
        
        if (args == null || args.length == 0) {
        	cliOptionsParser.printUsage();
        }

        try {
        	cliOptionsParser.parse(args);
        } catch(ParameterException e) {
        	System.out.println(e.getMessage());
        	cliOptionsParser.printUsage();
            System.exit(-1);
        }

        String parsedCommand = cliOptionsParser.getCommand();
        if (parsedCommand == null || parsedCommand.isEmpty()) {
            if (cliOptionsParser.getGeneralOptions().help) {
                cliOptionsParser.printUsage();
                System.exit(-1);
            }
            if (cliOptionsParser.getGeneralOptions().version) {
                System.out.println("version = 3.1.0");
            }
        } else {
            CommandExecutor commandExecutor = null;
            switch (parsedCommand) {
                case "fastq":
                    if (cliOptionsParser.getFastqCommandOptions().commonOptions.help) {
                        cliOptionsParser.printUsage();
                    } else {
                        commandExecutor = new FastqCommandExecutor(cliOptionsParser.getFastqCommandOptions());
                    }
                    break;
                case "bam":
                    if (cliOptionsParser.getBamCommandOptions().commonOptions.help) {
                        cliOptionsParser.printUsage();
                    } else {
                        commandExecutor = new BamCommandExecutor(cliOptionsParser.getBamCommandOptions());
                    }
                    break;
                case "ga4gh":
                    if (cliOptionsParser.getGa4ghCommandOptions().commonOptions.help) {
                        cliOptionsParser.printUsage();
                    } else {
                        commandExecutor = new Ga4ghCommandExecutor(cliOptionsParser.getGa4ghCommandOptions());
                    }
                    break;
                case "align":
                    if (cliOptionsParser.getAlignCommandOptions().commonOptions.help) {
                        cliOptionsParser.printUsage();
                    } else {
                        commandExecutor = new AlignCommandExecutor(cliOptionsParser.getAlignCommandOptions());
                    }
                    break;
                default:
                    break;
            }

            if (commandExecutor != null) {
                commandExecutor.execute();
            }
        }
    }
}
