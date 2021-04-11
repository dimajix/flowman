<template>
  <v-container>
    <v-card>
    <v-list nav>
      <v-list-item :to="{name: 'system'}">
        <v-list-item-action>
          <v-icon>info</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          System
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'namespace'}">
        <v-list-item-action>
          <v-icon>home</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Namespace
        </v-list-item-title>
      </v-list-item>

      <v-list-item :to="{name: 'project'}">
        <v-list-item-action>
          <v-icon>home</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Project
        </v-list-item-title>
      </v-list-item>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>work</v-icon>
            </v-list-item-action>
            <v-list-item-title>Jobs</v-list-item-title>
          </v-list-item>
        </template>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>work</v-icon>
            </v-list-item-action>
            <v-list-item-title>Targets</v-list-item-title>
          </v-list-item>
        </template>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>work</v-icon>
            </v-list-item-action>
            <v-list-item-title>Mappings</v-list-item-title>
          </v-list-item>
        </template>
      </v-list-group>
    </v-list>
    </v-card>
  </v-container>
</template>


<script>
export default {
  name: 'NavigationMenu',

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
