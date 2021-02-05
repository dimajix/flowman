<template>
  <v-container>
    <v-card>
      <v-card-title>Namespace {{name}}</v-card-title>
      <p>Profiles: {{ profiles }}</p>
      <p>Plugins: {{ plugins }}</p>
    </v-card>

    <v-card>
      <v-card-title>Environment</v-card-title>
      <v-data-table
        dense
        :headers="keyValueHeaders"
        :items="environment"
        :items-per-page="10"
        item-key="name"
        class="elevation-1"
      ></v-data-table>
    </v-card>

    <v-card>
      <v-card-title>Configs</v-card-title>
      <v-data-table
        dense
        :headers="keyValueHeaders"
        :items="config"
        :items-per-page="10"
        item-key="name"
        class="elevation-1"
      ></v-data-table>
    </v-card>
  </v-container>
</template>

<script>
  export default {
    data: () => ({
      name: "",
      environment: [],
      config: [],
      profiles: [],
      plugins: [],

      keyValueHeaders: [
        { text: 'Name', value: 'name' },
        { text: 'Value', value: 'value' }
      ]
    }),

    mounted() {
      this.getData()
    },

    methods: {
      getData() {
        this.loading = true
        this.$api.getNamespace()
          .then(response => {
            this.name = response.name
            this.environment = Object.entries(response.environment)
              .map(kv => { return {name:kv[0], value:kv[1]}})
              .sort((a,b) => (a.name.localeCompare(b.name)))
            this.config = Object.entries(response.config)
              .map(kv => { return {name:kv[0], value:kv[1]}})
              .sort((a,b) => (a.name.localeCompare(b.name)))
            this.profiles = response.profiles
            this.plugins = response.plugins
          })
      }
    }
  }
</script>

<style>

</style>
