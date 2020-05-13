# Argentina Congress Data Downloader

This script automatically retrieves the following datasets from the House of Representatives in Argentina:
1. COVID19 Subsidies
2. Laws
3. House of Representatives Sessions (Diputados)
4. List of Representatives (Diputados)

## Features
- The script automatically creates a SQLite database and allows for downloading any of the above datasets
- Support for API pagination
- Data schema detection. Tables are created dynamically following the specifications in the JSON API. 


## Known limitations
- The script will not work if the resource handlers in the Open Data government website change. Updating them on the script is trivial.
- 

This repository has all the code needed to create and manage Outline servers on
DigitalOcean. An Outline server runs instances of Shadowsocks proxies and
provides an API used by the Outline Manager application.

Go to https://getoutline.org for ready-to-use versions of the software.

## Components

The system comprises the following components:

- **Outline Server**: a proxy server that runs a Shadowsocks instance for each
  access key and a REST API to manage the access keys. The Outline Server runs
  in a Docker container in the host machine.

  See [`src/shadowbox`](src/shadowbox)

- **Outline Manager:** an [Electron](https://electronjs.org/) application that
  can create Outline Servers on the cloud and talks to their access key
  management API to manage who has access to the server.

  See [`src/server_manager`](src/server_manager)

- **Metrics Server:** a REST service that the Outline Server talks to
  if the user opts-in to anonymous metrics sharing.

  See [`src/metrics_server`](src/metrics_server)


## Code Prerequisites

In order to build and run the code, you need the following installed:
  - [Node](https://nodejs.org/)
  - [Yarn](https://yarnpkg.com/en/docs/install)
  - [Wine](https://www.winehq.org/download), if you would like to generate binaries for Windows.

Then you need to install all the NPM package dependencies:
```
yarn
```

Note: If you are using root (not recommended on your dev machine, maybe in a container), you need to add `{ "allow_root": true }` to your `/root/.bowerrc` file.

This project uses [Yarn workspaces](https://yarnpkg.com/blog/2017/08/02/introducing-workspaces/).


## Build System

We have a very simple build system based on package.json scripts that are called using `yarn`
and a thin wrapper for what we call build "actions".

We've defined a `do` package.json script that takes an `action` parameter:
```shell
yarn do $ACTION
```

This command will define a `do_action()` function and call `${ACTION}_action.sh`, which must exist.
The called action script can use `do_action` to call its dependencies. The $ACTION parameter is
always resolved from the project root, regardless of the caller location.

The idea of `do_action` is to keep the build logic next to where the relevant code is.
It also defines two environmental variables:

- ROOT_DIR: the root directory of the project, as an absolute path.
- BUILD_DIR: where the build output should go, as an absolute path.

### Build output

Building creates the following directories under `build/`:
- `web_app/`: The Manager web app.
  - `static/`: The standalone web app static files. This is what one deploys to a web server or runs with Electron.
- `electron_app/`: The launcher desktop Electron app
  - `static/`: The Manager Electron app to run with the electron command-line
  - `bundled/`: The Electron app bundled to run standalone on each platform
  - `packaged/`: The Electron app bundles packaged as single files for distribution
- `invite_page`: the Invite Page
  - `static`: The standalone static files to be deployed
- `shadowbox`: The Proxy Server

The directories have subdirectories for intermediate output:
- `ts/`: Autogenerated Typescript files
- `js/`: The output from compiling Typescript code
- `browserified/`: The output of browserifying the JavaScript code

To clean up:
```
yarn run clean
```
