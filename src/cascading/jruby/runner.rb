puts "Found #{Cascading::Cascade.all.size} Cascades in global registry"

Cascading::Cascade.all.each do |cascade|
  puts "runner.rb running '#{cascade.name}' Cascade"
  cascade.complete
end
