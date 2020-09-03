package main;

import java.io.File;
import java.io.IOException;

import bench.Benchmark;
import bench.Planner;
import kv_interfaces.KvInterface;
import main.Config.LibType;

public class Main {
	private static void printHelp() {
		System.out.println("Usage: xxx server/client/local config.yaml");
		System.exit(0);
	}

	public static void main(String[] args) {
		String configFile = null;
		if (args.length == 1) {
			System.out.println("[Warning]No config file is specified, using config.yaml by default");
			configFile = "config.yaml";
		} else if (args.length == 2) {
			configFile = args[1];
			System.out.println("using config file: " + configFile);
		} else {
			printHelp();
		}

		Config.readConfig(configFile);

		System.out.println("Your Config: \n" + Config.get().toString());

		if (args[0].equals("local")) {
			clearLogs();
			localMain();
		} else {
			printHelp();
		}
		System.exit(0);
	}
	
	private static void clearLogs() {
		assert Config.get().COBRA_FD_LOG != null;
		File f = new File(Config.get().COBRA_FD_LOG);
		assert f.isDirectory();
		
		for (File log : f.listFiles()) {
			boolean done = log.delete();
			System.out.println("[INFO] delete file [" + log.toString() + "]... done?" + done);
		}
		System.out.println("[INFO] Clear done.");
	}

	
	private static void localMain() {
		Planner.standardProcedure();
	}
	
}