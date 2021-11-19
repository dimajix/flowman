<template>
  <v-container
    fluid
  >
    <v-row>
      <v-col>
        <v-card>
          <v-card-title>Namespace "{{ name }}"</v-card-title>
          <v-card-text>
          <v-divider class="my-2"></v-divider>
          <v-item-group>
            <v-subheader>Profiles</v-subheader>
            <v-chip-group
              active-class="primary--text"
              column
            >
              <v-chip
                v-for="p in profiles "
                :key="p"
              >
                {{ p }}
              </v-chip>
            </v-chip-group>
          </v-item-group>

          <v-divider class="my-2"></v-divider>
          <v-item-group>
            <v-subheader>Plugins</v-subheader>
            <v-chip-group
              active-class="primary--text"
              column
            >
              <v-chip
                v-for="p in plugins"
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
