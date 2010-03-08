package cascading.jruby;

import org.jruby.Ruby;
import org.jruby.RubyInstanceConfig;

public class Main {

	/**
	 * Starts an Hadoop job by reading the specified JRuby script. The syntax is
	 * : <verbatim> hadoop jar myjob.jar cascading.jruby.JobRunner myjob.rb
	 * <input> <output> </verbatim>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String name = args[0]; // the main script name.
		if (!name.startsWith("/"))
			name = "/" + name;

		// Ruby script arguments:
		String[] newArgs = new String[args.length - 1];
		System.arraycopy(args, 1, newArgs, 0, args.length - 1);
		RubyInstanceConfig config = new RubyInstanceConfig();
		config.processArguments(newArgs);

		System.out.println("Arguments: ");
		for (String arg : config.getArgv())
			System.out.println(arg);

		Ruby runtime = Ruby.newInstance(config);

		runtime.executeScript("require '" + name + "'", name);
        // gfodor
		runtime.executeScript("require 'cascading/jruby/runner'", null);
	}
}
