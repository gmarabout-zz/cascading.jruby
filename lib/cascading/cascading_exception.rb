# Wrapper meant for NativeExceptions that wrap exceptions from Cascading.  The
# trouble is that the combined stack traces are so long, printing them case
# actually omit locations in the cascading.jruby or application code that
# matter, leaving you with no information about the source of the error.  This
# class just swallows all the nested exceptions, printing their message, while
# giving you a direct route into JRuby code to the cause of the problem.
class CascadingException < StandardError
  def initialize(native_exception, message)
    @ne = native_exception
    super("#{message}\n#{trace_causes(@ne, 1)}")
  end

  def cause(depth)
    fetch_cause(@ne, depth)
  end

  private

  def fetch_cause(ne, depth)
    return ne if depth <= 1
    fetch_cause(ne.cause, depth - 1)
  end

  def trace_causes(ne, depth)
    "Cause #{depth}: #{ne}\n#{trace_causes(ne.cause, depth + 1)}" if ne
  end
end
