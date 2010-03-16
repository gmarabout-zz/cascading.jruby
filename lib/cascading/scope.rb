module Cascading
  class Scope
    attr_accessor :scope, :grouping_key_fields, :primary_key_fields, :grouping_primary_key_fields
    @@scheme_keys = {}

    def initialize(scope, params = {})
      @scope = scope
      @grouping_key_fields = fields(params[:grouping_key_fields] || [])
      @primary_key_fields = fields(params[:primary_key_fields])
      @grouping_primary_key_fields = fields(params[:grouping_primary_key_fields])
    end

    def copy
      Scope.new(Java::CascadingFlow::Scope.new(@scope),
          :grouping_key_fields => @grouping_key_fields,
          :primary_key_fields => @primary_key_fields,
          :grouping_primary_key_fields => @grouping_primary_key_fields
      )
    end

    def self.register_scheme_key(scheme, primary_key)
      @@scheme_keys[scheme] = primary_key
    end

    def self.empty_scope(name)
      Scope.new(Java::CascadingFlow::Scope.new(name))
    end

    def self.tap_scope(tap, name)
      java_scope = outgoing_scope_for(tap, java.util.HashSet.new)
      # Taps and Pipes don't name their outgoing scopes like other FlowElements
      java_scope.name = name
      scope = Scope.new(java_scope,
          :primary_key_fields => @@scheme_keys[tap.scheme.class],
          :grouping_primary_key_fields => @@scheme_keys[tap.scheme.class]
      )
      vf, gf = scope.values_fields.to_a, scope.grouping_fields.to_a
      pk, gpk = scope.primary_key_fields.to_a, scope.grouping_primary_key_fields.to_a
      raise "Primary key must be a subset of available fields (primary key: #{pk.inspect}, values fields: #{vf.inspect})" unless vf & pk == pk
      raise "Grouping primary key must be a subset of available fields (grouping primary key: #{gpk.inspect}, grouping fields: #{gf.inspect})" unless gf & gpk == gpk
      scope
    end

    def self.outgoing_scope(flow_element, incoming_scopes, grouping_key_fields, every_applied)
      java_scopes = incoming_scopes.compact.map{ |s| s.scope }
      scope = Scope.new(outgoing_scope_for(flow_element, java.util.HashSet.new(java_scopes)),
          :grouping_key_fields => grouping_key_fields
      )
      scope.grouping_primary_key_fields = fields(grouping_primary_key_fields(flow_element, incoming_scopes, scope))
      scope.primary_key_fields = scope.grouping_primary_key_fields if every_applied
      scope.primary_key_fields = fields(primary_key_fields(flow_element, incoming_scopes, scope)) unless every_applied
      scope
    end

    def values_fields
      @scope.out_values_fields
    end

    def grouping_fields
      keys = @grouping_key_fields.to_a
      grouping_fields = @scope.out_grouping_fields.to_a
      # Overwrite key fields only
      fields(keys + grouping_fields[keys.size..-1])
    end

    def to_s
      kind = 'Unknown'
      kind = 'Tap'   if @scope.tap?
      kind = 'Group' if @scope.group?
      kind = 'Each'  if @scope.each?
      kind = 'Every' if @scope.every?
      <<-END
Scope name: #{@scope.name}
  Kind: #{kind}
  Argument selector: #{@scope.argument_selector}
  Declared fields: #{@scope.declared_fields}
  Grouping selectors: #{@scope.grouping_selectors}
  Sorting selectors: #{@scope.sorting_selectors}
  Out grouping
    selector: #{@scope.out_grouping_selector}
    fields: #{grouping_fields}
    key fields: #{@grouping_key_fields}
    primary key fields: #{@grouping_primary_key_fields}
  Out values
    selector: #{@scope.out_values_selector}
    fields: #{values_fields}
    primary key fields: #{@primary_key_fields}
END
    end

    private

    def self.outgoing_scope_for(flow_element, incoming_scopes)
      begin
        flow_element.outgoing_scope_for(incoming_scopes)
      rescue NativeException => e
        raise CascadingException.new(e, 'Exception computing outgoing scope')
      end
    end

    def self.primary_key_fields(flow_element, incoming_scopes, scope)
      case flow_element
        when Java::CascadingPipe::Each
          # assert incoming_scopes.size == 1
          project_primary_key(incoming_scopes.first.primary_key_fields,
                              incoming_scopes.first.values_fields.to_a,
                              scope.values_fields.to_a)
        when Java::CascadingPipe::Every
          # assert incoming_scopes.size == 1
          incoming_scopes.first.primary_key_fields
        when Java::CascadingPipe::GroupBy
          if incoming_scopes.size == 1
            incoming_scopes.first.primary_key_fields
          else
            # We must clear the primary key when unioning multiple inputs.  If
            # the programmer wants to preserve the primary key, they must use
            # the primary override.
            nil
          end
        when Java::CascadingPipe::CoGroup
          # FIXME: assume grouping_key_fields are the same for all
          # incoming_scopes.  Need join to give me names from all incoming
          # scopes to perform rename on primary key fields.
          union_fields(*incoming_scopes.map{ |s| s.primary_key_fields })
        else raise "No primary key rules for FlowElement of type #{flow_element}"
      end
    end

    def self.project_primary_key(primary_key, old_fields, new_fields)
      return nil if primary_key.nil?
      primary_key = primary_key.to_a
      primary_key if (primary_key & new_fields) == primary_key
    end

    def self.grouping_primary_key_fields(flow_element, incoming_scopes, scope)
      case flow_element
        when Java::CascadingPipe::Each
          # assert incoming_scopes.size == 1
          project_primary_key(incoming_scopes.first.grouping_primary_key_fields,
                              incoming_scopes.first.grouping_fields.to_a,
                              scope.grouping_fields.to_a)
        when Java::CascadingPipe::Every
          # assert incoming_scopes.size == 1
          incoming_scopes.first.grouping_primary_key_fields
        when Java::CascadingPipe::GroupBy
          scope.grouping_key_fields
        when Java::CascadingPipe::CoGroup
          scope.grouping_key_fields
        else raise "No primary key rules for FlowElement of type #{flow_element}"
      end
    end
  end

  # Register default primary keys
  begin
    Scope.register_scheme_key(Java::CascadingScheme::TextLine, ['offset'])
  rescue NameError => ne
    puts 'WARNING: Could not register primary key for TextLine Scheme as it was not on the class path'
  end
end
