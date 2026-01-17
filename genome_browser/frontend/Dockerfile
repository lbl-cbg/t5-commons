# Build Stage
FROM node:14-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm install --frozen-lockfile
COPY . .
RUN npm run build

# Developemnt Stage
FROM node:14-alpine AS development
WORKDIR /app
COPY package*.json ./
RUN npm install --frozen-lockfile
COPY . .
EXPOSE 3000
CMD ["npm", "start"]


# Production Stage
FROM nginx:stable-alpine AS production
RUN rm /etc/nginx/conf.d/default.conf
COPY /nginx/include.websocket /etc/nginx/app/include.websocket
COPY /nginx/include.forwarded /etc/nginx/app/include.forwarded
COPY /nginx/nginx.conf /etc/nginx/conf.d
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 8080
CMD ["nginx", "-g", "daemon off;"]










# pull official base image
#FROM node:lts-slim 

# set work directory
#WORKDIR /srv/app/

# add to $PATH
#ENV PATH /srv/app/node_modules/.bin:$PATH

# install app dependencies
#COPY package.json ./
#COPY package-lock.json ./
#COPY . ./
#RUN npm install
#RUN npm run build

# add app
#COPY . ./
