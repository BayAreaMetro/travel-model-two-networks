#docker build -t shst .
#docker run -it --rm -v /local/path:/usr/node/ shst:latest /bin/bash
#docker run -it --rm -v /local/path:/usr/node/ shst:latest shst extract usr/node/boundary_ii.geojson --out=usr/node/ii.geojson --metadata --tile-hierarchy=8 --tiles
#docker run -it --rm -v /local/path:/usr/node/ shst:latest shst match /usr/node/point_1.in.geojson --out=/usr/node/out.geojson --tile-hierarchy=8

FROM node:12-bullseye-slim

ENV NPM_CONFIG_PREFIX=/home/node/.npm-global
ENV PATH=$PATH:/home/node/.npm-global/bin

# give node (shst) more memory
ENV NODE_OPTIONS=--max_old_space_size=8192
# note: if running docker on windows, give your virtual machine more memory as well:
# for example, you may need to create a .wslconfig file with memory=16GB
# https://docs.microsoft.com/en-us/windows/wsl/wsl-config#configure-global-options-with-wslconfig

USER node
RUN npm install -g sharedstreets@0.12.4
