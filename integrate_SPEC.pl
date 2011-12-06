#!/usr/bin/perl
# INTEGRATE SPECjvm2008 INTO CLOUDCMP

use strict;

# source files we need from SPEC
my %files = (
    "compress/Compress.java", "compress/Compress.java",
    "compress/Harness.java", "compress/CompressTask.java",
    "crypto/Util.java", "crypto/Util.java",
    "crypto/aes/Main.java", "crypto/aes/CryptoAESTask.java",
    "crypto/rsa/Main.java", "crypto/rsa/CryptoRSATask.java",
    "crypto/signverify/Main.java", "crypto/signverify/CryptoSignverifyTask.java",
    "mpegaudio/Main.java", "mpegaudio/MpegaudioTask.java",
    "scimark/fft/FFT.java", "scimark/fft/ScimarkFFTTask.java",
    "scimark/lu/LU.java", "scimark/lu/ScimarkLUTask.java",
    "scimark/monte_carlo/MonteCarlo.java", "scimark/monte_carlo/ScimarkMonteCarloTask.java",
    "scimark/sor/SOR.java", "scimark/sor/ScimarkSORTask.java",
    "scimark/sparse/SparseCompRow.java", "scimark/sparse/ScimarkSparseTask.java",
    "scimark/utils/*", "scimark/utils/",
    "serial/Main.java", "serial/SerialTask.java",
    "serial/Utils.java", "serial/Utils.java",
    "serial/data/*", "serial/data/",
    "sunflow/Main.java", "sunflow/SunflowTask.java");

# patch file to use
my $patch = "SPEC.patch";

@ARGV > 0 or die "[USAGE] ./integrate_SPEC [SPEC installation directory]";

my $spec_dir = $ARGV[0];
(-e "$spec_dir/run-specjvm.cmd") or die "Cannot find the SPEC installation!";

my $bench_dir = "$spec_dir/src/spec/benchmarks";

print "Copying source files from SPEC..\n";
foreach my $file (keys(%files)) {
    my $from = "$bench_dir/$file";
    my $to = "src/org/cloudcmp/tasks/compute/".$files{$file};
    `cp -r $from $to`;
}

# resource files from SPEC
print "Copying resource files from SPEC..\n";
`cp -r $spec_dir/resources WebContent/`;

# patch the source
print "Patching source files..\n";
`patch -p0 -i $patch`;

print "Done!\n";
