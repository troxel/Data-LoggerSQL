use 5.006;
use strict;
use warnings;
use inc::Module::Install;

name     'Data-LoggerSQL';
all_from 'lib/Data/LoggerSQL.pm';
author   q{Steven Troxel <troxel@perlworks.com>};
license  'artistic2';

perl_version 5.006;

tests_recursive('t');

resources (
   #homepage   => 'http://yourwebsitehere.com',
   #IRC        => 'irc://irc.perl.org/#Data-LoggerSQL',
   license    => 'http://www.perlfoundation.org/artistic_license_2_0',
   #repository => 'git://github.com/Steven Troxel/Data-LoggerSQL.git',
   #repository => 'https://bitbucket.org/Steven Troxel/Data-LoggerSQL',
   bugtracker => 'http://rt.cpan.org/NoAuth/Bugs.html?Dist=Data-LoggerSQL',
);

configure_requires (
   'Module::Install' => 0,
);

build_requires (
   'Test::More' => 0,
);

requires (
   #'ABC'              => 1.6,
   #'Foo::Bar::Module' => 5.0401,
);

install_as_cpan;
auto_install;
WriteAll;
