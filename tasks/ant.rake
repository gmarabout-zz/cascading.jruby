namespace :ant do
  desc 'Builds Java source for inclusion in gem'
  task :build do
    `ant build`
  end

  desc 'Cleans Java build files'
  task :clean do
    `ant clean`
  end
end
