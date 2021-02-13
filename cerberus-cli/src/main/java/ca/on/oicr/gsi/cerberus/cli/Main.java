package ca.on.oicr.gsi.cerberus.cli;

import java.util.concurrent.Callable;
import picocli.CommandLine;

/** Main entry point from the command line */
@CommandLine.Command(
    name = "cerberus",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "TSV file provenance generator")
public final class Main implements Callable<Integer> {

  public static void main(String[] args) {
    final var cmd = new CommandLine(new Main()).addSubcommand("online", new RunOnline());
    cmd.setExecutionStrategy(new CommandLine.RunLast());
    System.exit(cmd.execute(args));
  }

  @Override
  public Integer call() {
    System.err.println("Please specify a command or --help to see what commands are available.");
    return 1;
  }
}
