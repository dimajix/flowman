<template>
  <v-container>
    <v-card>
      <v-card-title>Project {{name}}</v-card-title>
      <v-card-subtitle>{{description}}</v-card-subtitle>
      <p>Project Name: {{ name }}</p>
      <p>Project Version: {{ version }}</p>
      <p>Basedir: {{ basedir }}</p>
      <p>Jobs: {{ jobs }}</p>
      <p>Targets: {{ targets }}</p>
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
    props: {
      projectName: {
      }
    },

    data: () => ({
      name: "",
      version: "",
      description: null,
      basedir: null,
      environment: [],
      config: [],
      jobs: [],
      targets: [],

      keyValueHeaders: [
        { text: 'Name', value: 'name' },
        { text: 'Value', value: 'value' }
      ]
    }),

    mounted() {
      this.getData()
    },

    watch: {
      '$route' () {
        this.getData()
      }
    },

    methods: {
      getData() {
        this.$api.getProject(this.projectName)
          .then(response => {
            this.name = response.name
            this.version = response.version
            this.description = response.description
            this.basedir = response.basedir
            this.environment = Object.entries(response.environment)
              .map(kv => { return {name:kv[0], value:kv[1]}})
              .sort((a,b) => (a.name.localeCompare(b.name)))
            this.config = Object.entries(response.config)
              .map(kv => { return {name:kv[0], value:kv[1]}})
              .sort((a,b) => (a.name.localeCompare(b.name)))
            this.jobs = response.jobs
            this.targets = response.targets
          })
      }
    }
  }
</script>

<style>

</style>
