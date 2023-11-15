{
  description = "Ethereum network crawler, API, and frontend";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";

    devshell = {
      url = "github:numtide/devshell";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
    };
    gitignore = {
      url = "github:hercules-ci/gitignore.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    templ = {
      url = "github:a-h/templ";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs@{
    self,
    nixpkgs,
    devshell,
    flake-parts,
    gitignore,
    ...
  }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        devshell.flakeModule
        flake-parts.flakeModules.easyOverlay
      ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      perSystem = { config, pkgs, system, ... }: let
        inherit (gitignore.lib) gitignoreSource;
        templ = inputs.templ.packages.${system}.templ;
      in {
        # Attrs for easyOverlay
        overlayAttrs = {
          inherit (config.packages)
            nodeCrawler
            nodeCrawlerFrontend;
        };

        packages = {
          nodeCrawler = pkgs.buildGo121Module {
            pname = "crawler";
            version = "0.0.0";

            src = gitignoreSource ./.;
            subPackages = [ "cmd/crawler" ];

            vendorHash = "sha256-0rRu81KnbBWICPvE6m2/OkXifOO+uW/SYSIFCVHM3YY=";

            doCheck = false;

            CGO_ENABLED = 0;

            preBuild = ''
              ${templ}/bin/templ generate
            '';

            ldflags = [
              "-s"
              "-w"
              "-extldflags -static"
            ];
          };
          nodeCrawlerFrontend = pkgs.buildNpmPackage {
            pname = "frontend";
            version = "0.0.0";

            src = gitignoreSource ./frontend;

            npmDepsHash = "sha256-1nLQVoNkiA4x97UcPe8rNMXa7bYCskazpJesWVLnDHk=";

            installPhase = ''
              mkdir -p $out/share
              cp -r build/ $out/share/frontend
            '';
          };
        };

        devshells.default = {
          commands = [
            {
              name = "go-mod-upgrade";
              help = "Upgrades the go dependencies. Prints the new vendorHash.";
              command = ''
                go get -u ./... && \
                go mod tidy && \
                nix-prefetch --option extra-experimental-features flakes --silent \
                  '{ sha256 }: (builtins.getFlake (toString ./.)).packages.x86_64-linux.nodeCrawler.goModules.overrideAttrs (_: { vendorSha256 = sha256; })'
              '';
            }
          ];
          packages = with pkgs; [
            nix-prefetch
            go_1_21
            golangci-lint
            nodejs
            sqlite-interactive
            templ
          ];
        };
      };

      flake = rec {
        nixosModules.default = nixosModules.nodeCrawler;
        nixosModules.nodeCrawler = { config, lib, pkgs, ... }:
        with lib;
        let
          cfg = config.services.nodeCrawler;
          apiAddress = "${cfg.api.address}:${toString cfg.api.port}";
        in
        {
          options.services.nodeCrawler = {
            enable = mkEnableOption (self.flake.description);

            hostName = mkOption {
              type = types.str;
              default = "localhost";
              description = "Hostname to serve Node Crawler on.";
            };

            nginx = mkOption {
              type = types.attrs;
              default = { };
              example = literalExpression ''
                {
                  forceSSL = true;
                  enableACME = true;
                }
              '';
              description = "Extra configuration for the vhost. Useful for adding SSL settings.";
            };

            stateDir = mkOption {
              type = types.path;
              default = /var/lib/node_crawler;
              description = "Directory where the databases will exist.";
            };

            crawlerDatabaseName = mkOption {
              type = types.str;
              default = "crawler.db";
              description = "Name of the file within the `stateDir` for storing the data for the crawler.";
            };

            backupFilename = mkOption {
              type = types.str;
              default = "backups/crawler_20060102150405.db";
              description = "Daily backup filename.";
            };

            user = mkOption {
              type = types.str;
              default = "nodecrawler";
              description = "User account under which Node Crawler runs.";
            };

            group = mkOption {
              type = types.str;
              default = "nodecrawler";
              description = "Group account under which Node Crawler runs.";
            };

            dynamicUser = mkOption {
              type = types.bool;
              default = true;
              description = ''
                Runs the Node Crawler as a SystemD DynamicUser.
                It means SystenD will allocate the user at runtime, and enables
                some other security features.
                If you are not sure what this means, it's safe to leave it default.
              '';
            };

            api = {
              enable = mkOption {
                default = true;
                type = types.bool;
                description = "Enables the Node Crawler API server.";
              };

              pprof = mkOption {
                type = types.bool;
                default = false;
                description = "Enable the pprof http server";
              };

              address = mkOption {
                type = types.str;
                default = "127.0.0.1";
                description = "Listen address for the API server.";
              };

              port = mkOption {
                type = types.port;
                default = 10000;
                description = "Listen port for the API server.";
              };

              metricsAddress = mkOption {
                type = types.str;
                default = "0.0.0.0:9190";
                description = "Address on which the metrics server listens. This is NOT added to the firewall.";
              };

              enodePubkey = mkOption {
                type = types.str;
                default = "";
                description = "Public key of the crawler. Use the CLI to get it: `crawler print-enode node.key host port`";
                example = "0401edb73871c1ce0ebc2203bbc12c7b3b3a2d57fc72533f...";
              };

              enode = mkOption {
                type = types.str;
                default = "enode://${cfg.api.enodePubkey}@${cfg.hostName}:${toString cfg.crawler.nodeListenPort}";
                description = "Enode of the crawler. Used in the /help page to show how to connect to the node.";
              };
            };

            crawler = {
              enable = mkOption {
                default = true;
                type = types.bool;
                description = "Enables the Node Crawler API server.";
              };

              pprof = mkOption {
                type = types.bool;
                default = false;
                description = "Enable the pprof http server";
              };

              geoipdb = mkOption {
                type = types.path;
                default = config.services.geoipupdate.settings.DatabaseDirectory + "/GeoLite2-City.mmdb";
                description = ''
                  Location of the GeoIP database.

                  If the default is used, the `geoipupdate` service files.
                  So you will need to configure it.
                  Make sure to enable the `GeoLite2-City` edition.

                  If you do not want to enable the `geoipupdate` service, then
                  the `GeoLite2-City` file needs to be provided.
                '';
              };

              network = mkOption {
                type = types.str;
                default = "mainnet";
                example = "holesky";
                description = "Name of the network to crawl. Defaults to Mainnet.";
              };

              openFirewall = mkOption {
                type = types.bool;
                default = true;
                description = "Opens the crawler ports.";
              };

              workers = mkOption {
                type = types.int;
                default = 16;
                description = "Number of crawler workers to start.";
              };

              metricsAddress = mkOption {
                type = types.str;
                default = "0.0.0.0:9191";
                description = "Address on which the metrics server listens. This is NOT added to the firewall.";
              };

              nextCrawlSuccess = mkOption {
                type = types.str;
                default = "12h";
                description = "Next crawl value if the crawl was successful.";
              };

              nextCrawlFail = mkOption {
                type = types.str;
                default = "48h";
                description = "Next crawl value if the crawl was unsuccessful.";
              };

              nextCrawlNotEth = mkOption {
                type = types.str;
                default = "336h"; # 14d
                description = "Next crawl value if the node was not an eth node.";
              };

              nodeListenPort = mkOption {
                type = types.port;
                default = 30303;
                description = "Port number for the node listen address.";
              };
            };
          };

          config = mkIf cfg.enable {
            networking.firewall = mkIf cfg.crawler.openFirewall {
              allowedUDPPorts = [ cfg.crawler.nodeListenPort ];
              allowedTCPPorts = [ cfg.crawler.nodeListenPort ];
            };

            systemd.services = {
              node-crawler-crawler = mkIf cfg.crawler.enable {
                description = "Node Cralwer, the Ethereum Node Crawler.";
                wantedBy = [ "multi-user.target" ];
                after = [ "network.target" ];

                serviceConfig = {
                  ExecStart =
                  let
                    args = [
                      "--backup-name=${cfg.backupFilename}"
                      "--crawler-db=${cfg.crawlerDatabaseName}"
                      "--geoipdb=${cfg.crawler.geoipdb}"
                      "--metrics-addr=${cfg.crawler.metricsAddress}"
                      "--next-crawl-fail=${cfg.crawler.nextCrawlFail}"
                      "--next-crawl-not-eth=${cfg.crawler.nextCrawlNotEth}"
                      "--next-crawl-success=${cfg.crawler.nextCrawlSuccess}"
                      "--node-addr=0.0.0.0:${toString cfg.crawler.nodeListenPort}"
                      "--workers=${toString cfg.crawler.workers}"
                    ]
                    ++ optional (cfg.crawler.network == "goerli") "--goerli"
                    ++ optional (cfg.crawler.network == "holesky") "--holesky"
                    ++ optional (cfg.crawler.network == "sepolia") "--sepolia";
                  in
                  "${pkgs.nodeCrawler}/bin/crawler --pprof=${if cfg.crawler.pprof then "true" else "false"} crawl ${concatStringsSep " " args}";

                  WorkingDirectory = cfg.stateDir;
                  StateDirectory = optional (cfg.stateDir == /var/lib/node_crawler) "node_crawler";

                  DynamicUser = cfg.dynamicUser;
                  Group = cfg.group;
                  User = cfg.user;

                  Restart = "on-failure";
                };
              };
              node-crawler-api = mkIf cfg.api.enable {
                description = "Node Cralwer API, the API for the Ethereum Node Crawler.";
                wantedBy = [ "multi-user.target" ];
                after = [ "network.target" ]
                  ++ optional cfg.crawler.enable "node-crawler-crawler.service";

                serviceConfig = {
                  ExecStart =
                  let
                    args = [
                      "--api-addr=${apiAddress}"
                      "--backup-name=${cfg.backupFilename}"
                      "--crawler-db=${cfg.crawlerDatabaseName}"
                      "--enode=${cfg.api.enode}"
                      "--metrics-addr=${cfg.api.metricsAddress}"
                    ];
                  in
                  "${pkgs.nodeCrawler}/bin/crawler --pprof=${if cfg.api.pprof then "true" else "false"} api ${concatStringsSep " " args}";

                  WorkingDirectory = cfg.stateDir;
                  StateDirectory = optional (cfg.stateDir == /var/lib/node_crawler) "node_crawler";

                  DynamicUser = cfg.dynamicUser;
                  Group = cfg.group;
                  User = cfg.user;

                  Restart = "on-failure";
                };
              };
            };

            services.nginx = {
              enable = true;
              upstreams.nodeCrawlerApi.servers."${apiAddress}" = { };
              virtualHosts."${cfg.hostName}" = mkMerge [
                cfg.nginx
                {
                  locations = {
                    "/" = {
                      proxyPass = "http://nodeCrawlerApi/";
                    };
                  };
                }
              ];
            };
          };
        };
      };
  };
}
