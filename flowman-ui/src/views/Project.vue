<template>
  <v-container>
    <v-row>
      <v-col>
        <v-card>
          <v-card-title>Project "{{name}}" - (version {{ version }})</v-card-title>
          <v-card-subtitle>{{description}}</v-card-subtitle>
          <v-card-text>
            <v-text-field
              filled
              label="Basedir"
              :value="basedir"
              readonly
            ></v-text-field>

          <v-divider class="my-2"></v-divider>
          <v-item-group>
            <v-subheader>Jobs</v-subheader>
            <v-chip-group
              active-class="primary--text"
              column
            >
              <v-chip
                v-for="p in jobs"
                :key="p"
              >
                {{ p }}
              </v-chip>
            </v-chip-group>
          </v-item-group>

          <v-divider class="my-2"></v-divider>
          <v-item-group>
            <v-subheader>Targets</v-subheader>
            <v-chip-group
              active-class="primary--text"
              column
            >
              <v-chip
                v-for="p in targets"
                :key="p"
              >
                {{ p }}
              </v-chip>
            </v-chip-group>
          </v-item-group>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-row>
      <v-col>
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
      </v-col>
    </v-row>

    <v-row>
      <v-col>
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
      </v-col>
    </v-row>
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
