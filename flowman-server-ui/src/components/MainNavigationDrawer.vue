<template>
  <v-navigation-drawer
    app
    floating
    permanent
  >
    <v-list-item>
      <v-list-item-content>
        <v-list-item-title class="title">
          Flowman
        </v-list-item-title>
      </v-list-item-content>
    </v-list-item>

    <v-divider></v-divider>

    <v-list nav>
      <v-list-item :to="{name: 'home'}">
        <v-list-item-action>
          <v-icon>home</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Home
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'namespace'}">
        <v-list-item-action>
          <v-icon>info</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Namespace
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'job-history'}">
        <v-list-item-action>
          <v-icon>history</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Job History
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'target-history'}">
        <v-list-item-action>
          <v-icon>history</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Target History
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'metrics'}">
        <v-list-item-action>
          <v-icon>timeline</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Metrics
        </v-list-item-title>
      </v-list-item>
    </v-list>
  </v-navigation-drawer>
</template>

<script>
  export default {
    name: 'MainNavigationDrawer',

    data() {
      return {
      }
    },

    mounted() {
    },

    methods: {
    }
  }
</script>
