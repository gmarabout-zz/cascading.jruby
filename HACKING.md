# Hacking

Some hacking info on `cascading.jruby`:

`cascading.jruby` can be packaged as a gem. To do so, you must generate the necessary packaging files:

    ant build; jruby -S rake gem

will produce the gem in the pkg/ sub-directory. After that, just cd to this directory and:

    jruby -S rake install cascading.jruby-xxx.gem

The `Cascading::Operations` module is mixed-in the `Cascading::Assembly` class to provide some shortcuts for common operations.

The high level commands for creating new pipes are defined in cascading/helpers.rb.

The file cascading/cascading.rb defines global helper methods for cascading like tap creation, fields creation, etc. 
