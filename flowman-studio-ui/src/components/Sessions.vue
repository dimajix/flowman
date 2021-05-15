<template>
  <v-dialog
    v-model="show"
  >
    <template v-slot:activator="slotProps">
      <slot name="activator" v-bind="slotProps"></slot>
    </template>
    <v-card elevation="2" class="flex-fill">
      <v-card-title>
        <v-col>Running Kernels and Sessions</v-col>
        <v-col class="text-right"><v-icon @click="retrieveSessions()">refresh</v-icon></v-col>
      </v-card-title>
      <v-card-text>
        <v-treeview
          open-all
          rounded
          hoverable
          item-key="id"
          item-children="sessions"
          item-text="description"
          :items="sessions"
        >
          <template v-slot:append="{ item }">
            <v-row>
              <v-col v-if="item.session">
                <v-btn
                >Jump to Session {{item.id}}</v-btn>
              </v-col>
              <v-col v-if="!item.session">
                <v-chip>{{item.state}}</v-chip>
              </v-col>
              <v-col v-if="!item.session">
                <v-btn
                >New Session</v-btn>
              </v-col>
              <v-col v-if="!item.session">
                <v-btn
                  @click.stop="shutdownKernel(item.id)"
                >Shutdown</v-btn>
              </v-col>
            </v-row>
          </template>
        </v-treeview>
        <v-btn
          @click.stop="launchKernel()"
        >Start new Kernel</v-btn>
      </v-card-text>
    </v-card>
  </v-dialog>
</template>


<script>
export default {
  name: 'Kernels',
  components: {},
  data: () => ({
    sessions: [],
    show: false
  }),

  mounted() {
    this.retrieveSessions()
  },

  watch: {
    // whenever question changes, this function will run
    show: function (newValue) {
      if (newValue) {
        this.retrieveSessions()
      }
    }
  },

  methods: {
    launchKernel() {
      this.$api.launchKernel().then(response => {
        response // Eat response
        this.retrieveSessions()
      })
    },
    shutdownKernel(kernel) {
      this.$api.shutdownKernel(kernel).then(response => {
        response // Eat response
        this.retrieveSessions()
      })
    },
    retrieveSessions() {
      this.$api.listKernels()
        .then(response => {
          this.sessions = response.kernels.map(k => {
              let result = {
                id: k.id,
                description: "Kernel " + k.id + " running at " + k.url,
                state: k.state,
                sessions: []
              }
              try {
                this.$api.listSessions(k.id).then(response => {
                  response.sessions.map(s => {
                    result.sessions.append({
                      id: s.id,
                      description: "Sessions " + s.id
                    })
                  })
                })
              } catch (error) {
                error
              }
              return result
            }
          )
          return this.sessions
        })
    }
  }

};
</script>
