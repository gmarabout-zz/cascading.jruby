package cascading.jruby;

import org.jruby.Ruby;
import org.jruby.RubyInstanceConfig;

public class Main {
    private final static String JRUBY_HOME = "/opt/jruby";

    /**
     * Starts a Hadoop job by reading the specified JRuby script.
     *
     * @param args
     */
    public static void main(String[] args) {
        String name = args[0]; // c.j script name
        if (!name.startsWith("/"))
            name = "/" + name;

        // c.j script args
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, args.length - 1);
        RubyInstanceConfig config = new RubyInstanceConfig();
        config.setJRubyHome(JRUBY_HOME); // mwalker
        config.processArguments(newArgs);

        System.out.println("Arguments: ");
        for (String arg : config.getArgv())
            System.out.println(arg);

        Ruby runtime = Ruby.newInstance(config);

        System.out.println("Requiring '" + name + "'");
        runtime.executeScript("require '" + name + "'", name);

        System.out.println("Requiring 'cascading/jruby/runner'");
        runtime.executeScript("require 'cascading/jruby/runner'", "runner"); // gfodor
    }
}
