{ config, pkgs, lib, ... }:
{
  languages.java = {
    enable = true;
    jdk.package = pkgs.jdk17_headless;
  };
  languages.python = {
    enable = true;
    uv.enable = true;
    uv.sync.enable = true;
  };

  services.kafka.enable = true;

  env.LD_LIBRARY_PATH = lib.makeLibraryPath [ pkgs.zlib pkgs.stdenv.cc.cc.lib ];
  env.STREAM_PROJECT_DIR = config.devenv.root;
  env.STREAM_DATA_DIR = "${config.devenv.root}/data";
  env.KAFKA_BOOTSTRAP = "localhost:9092";
  env.KAFKA_TOPIC = "wikipedia.recentchange";
  env.SPARK_DRIVER_MEMORY = "4g";
}
