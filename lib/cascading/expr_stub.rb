class ExprStub
  attr_accessor :expression, :types

  def initialize(st)
    @expression = st.dup
    @types = {}

    # Simple regexp based parser for types

    JAVA_TYPE_MAP.each do |sym, klass|
      @expression.gsub!(/[A-Za-z0-9_]+:#{sym.to_s}/) do |match|
        name = match.split(/:/).first.gsub(/\s+/, "")
        @types[name] = klass
        match.gsub(/:#{sym.to_s}/, "")
      end
    end
  end

  def self.split_hash(h)
    keys, values = h.keys.sort, []
    keys.each do |key|
      values << h[key]
    end
    [keys, values]
  end

  def self.split_names_and_types(expr_types)
    names, types = split_hash(expr_types)
    names = names.to_java(java.lang.String)
    types = types.to_java(java.lang.Class)
    [names, types]
  end
end
