<template>
  <v-navigation-drawer
    app
    clipped
    :mini-variant="!expand"
  >
    <v-list nav>
      <v-list-item :to="{name: 'home'}">
        <v-list-item-action>
          <v-icon>home</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Home
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'job-history', params: { project: project }}">
        <v-list-item-action>
          <v-icon>history</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Job History
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'target-history', params: { project: project }}">
        <v-list-item-action>
          <v-icon>history</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Target History
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'metrics', params: { project: project }}">
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

    props: {
      expand: {
        type: Boolean,
        default: () => true
      },
      project: {
        type: String,
        default: () => ""
      }
    },

    data() {
      return {
      }
    },

    mounted() {
    },
  }
</script>
