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

package org.opencb.hpg.bigdata.app.cli.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.launcher.SparkLauncher;
import org.ga4gh.models.Variant;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetMR;
import org.opencb.hpg.bigdata.tools.variant.Variant2HbaseMR;
import org.opencb.hpg.bigdata.tools.variant.Vcf2AvroMR;
import org.opencb.hpg.bigdata.tools.variant.spark.InputStreamReaderRunnable;

import java.net.URI;

/**
 * Created by imedina on 25/06/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private CliOptionsParser.VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(CliOptionsParser.VariantCommandOptions variantCommandOptions) {
//      super(fastqCommandOptions.logLevel, fastqCommandOptions.verbose, fastqCommandOptions.conf);
        this.variantCommandOptions = variantCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        String subCommandString = variantCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "convert":
                init(variantCommandOptions.convertVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.convertVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.convertVariantCommandOptions.commonOptions.conf);
                convert();
            case "index":
                init(variantCommandOptions.indexVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.indexVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.indexVariantCommandOptions.commonOptions.conf);
                index();
                break;
            case "ibs":
                init(variantCommandOptions.ibsVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.ibsVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.ibsVariantCommandOptions.commonOptions.conf);
                ibs();
                break;
            default:
                break;
        }
    }

    private void index() throws Exception {
        String input = variantCommandOptions.indexVariantCommandOptions.input;
        String db = variantCommandOptions.indexVariantCommandOptions.database;
        boolean nonVar = variantCommandOptions.indexVariantCommandOptions.includeNonVariants;
        boolean expand = variantCommandOptions.indexVariantCommandOptions.expand;

        URI server = null;
        // new URI("//who1:60000/VariantExpanded");
        if (StringUtils.isNotBlank(db)) {
            server = new URI(db);
        }
        Variant2HbaseMR.Builder builder = new Variant2HbaseMR.Builder(input, server);
        builder.setExpand(expand);
        builder.setNonVar(nonVar);
        Job job = builder.build(true);

        boolean fine = job.waitForCompletion(true);
        if (!fine) {
            throw new IllegalStateException("Variant 2 HBase failed!");
        }
    }


    private void convert() throws Exception {
        String input = variantCommandOptions.convertVariantCommandOptions.input;
        String output = variantCommandOptions.convertVariantCommandOptions.output;
        String compression = variantCommandOptions.convertVariantCommandOptions.compression;

        if (output == null) {
            output = input;
        }

        // clean paths
//        String in = PathUtils.clean(input);
//        String out = PathUtils.clean(output);

        if (variantCommandOptions.convertVariantCommandOptions.toParquet) {
            logger.info("Transform {} to parquet", input);

            new ParquetMR(Variant.getClassSchema()).run(input, output, compression);
//            if (PathUtils.isHdfs(input)) {
//                new ParquetMR(Variant.getClassSchema()).run(input, output, compression);
//            } else {
//                new ParquetConverter<Variant>(Variant.getClassSchema()).toParquet(new FileInputStream(input), output);
//            }

        } else {
            Vcf2AvroMR.run(input, output, compression);
        }
    }


    private void ibs() throws Exception {
        String input = variantCommandOptions.ibsVariantCommandOptions.input;
        String sparkHome = variantCommandOptions.ibsVariantCommandOptions.sparkHome;
        String appResource = variantCommandOptions.ibsVariantCommandOptions.appResource;

        SparkLauncher sparkLauncher = new SparkLauncher();
        Process sparkIBSClustering = sparkLauncher
                .setSparkHome(sparkHome)
                .setAppResource(appResource)
                .setMaster("local[*]")
                .setMainClass("org.opencb.hpg.bigdata.tools.variant.spark.SparkIBSClustering")
                .addAppArgs(input)
                .setVerbose(false)
                .launch();

//        sparkIBSClustering.waitFor();
        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(sparkIBSClustering.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(sparkIBSClustering.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        System.out.println("Waiting for finish...");
        int exitCode = sparkIBSClustering.waitFor();
        System.out.println("Finished! Exit code:" + exitCode);
    }

}
