<template>
  <v-app>
    <v-app-bar app color="primary">
      <v-app-bar-title>Flowman</v-app-bar-title>
    </v-app-bar>
    <main-navigation-drawer/>

    <v-main>
      <v-container class="fill-height" fluid>
        <v-row class="fill-height">
          <v-col>
            <router-view></router-view>
          </v-col>
        </v-row>
      </v-container>
    </v-main>

    <template>
      <v-footer class="pa-3" app>
        <v-spacer></v-spacer>
        <div>&copy; {{ new Date().getFullYear() }}</div>
      </v-footer>
    </template>
  </v-app>
</template>

<script>
import MainNavigationDrawer from './components/MainNavigationDrawer'

export default {
  name: 'App',
  components: {
      MainNavigationDrawer
  }
};
</script>
