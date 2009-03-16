# ext.rb : some extensions to basic types
#
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

class Array
  def extract_options!
     last.is_a?(::Hash) ? pop : {}
  end
  
  def extract_options
     last.is_a?(::Hash) ? last : {}
  end
end