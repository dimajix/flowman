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
        <v-col class="text-right"><v-icon @click="fetchKernels()">refresh</v-icon></v-col>
      </v-card-title>
      <v-card-text>
        <v-treeview
          rounded
          hoverable
          item-key="id"
          item-children="children"
          item-text="description"
          :items="items"
          :open="open"
        >
          <template v-slot:append="{ item }">
            <v-row v-if="item.kind === 'session'">
              <v-col>
                <v-btn
                >Jump to Session</v-btn>
              </v-col>
              <v-col>
                <v-btn
                >Close Project</v-btn>
              </v-col>
            </v-row>
            <v-row v-if="item.kind === 'kernel'">
              <v-col>
                <v-chip>{{item.state}}</v-chip>
              </v-col>
              <v-col>
                <project-selector :kernel="item.id">
                  <template v-slot:activator="{ on, attrs }">
                    <v-btn
                      v-bind="attrs"
                      v-on="on"
                    >
                      Open Project
                    </v-btn>
                  </template>
                </project-selector>
              </v-col>
              <v-col>
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
import ProjectSelector from '@/components/ProjectSelector'

export default {
  name: 'Kernels',
  components: {
    ProjectSelector
  },

  data: () => ({
    items: [],
    show: false,
  }),

  computed: {
    open() {
      // Forcibly reopen all elements whenever items change
      return this.items.map(k => {return k.id})
    }
  },

  mounted() {
  },

  watch: {
    // whenever question changes, this function will run
    show: function (newValue) {
      if (newValue) {
        this.fetchKernels()
      }
    }
  },

  methods: {
    launchKernel() {
      this.$api.launchKernel().then(response => {
        response // Eat response
        this.$api.setCurrentSession(response.id, "")
        this.fetchKernels()
      })
    },
    shutdownKernel(kernel) {
      this.$api.shutdownKernel(kernel).then(response => {
        response // Eat response
        this.fetchKernels()
      })
    },
    fetchKernels() {
      this.$api.listKernels()
        .then(response => {
          this.items = []
          response.kernels.forEach(k => {
            return this.$api.listSessions(k.id).then(response => {
              return response.sessions.map(s => {
                return {
                  id: s.id,
                  kind: "session",
                  description: "Project " + s.project
                }
              })
            })
            .catch(error => {
              error
              return []
            })
            .then(s => {
              this.items.push({
                id: k.id,
                kind: "kernel",
                description: "Kernel " + k.id + " running at " + k.url,
                state: k.state,
                children: s
              })
            })
          })
        })
    },
    fetchSessions(kernel) {
      return this.$api.listSessions(kernel.id).then(response => {
        return response.sessions.map(s => {
          return {
            id: s.id,
            kind: "session",
            description: "Project " + s.project
          }
        })
      })
      .catch(error => {
        error
        return []
      })
    }
  }
};
</script>
