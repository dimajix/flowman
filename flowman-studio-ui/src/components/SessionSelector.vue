<template>
  <v-dialog
    v-model="show"
  >
    <template v-slot:activator="slotProps">
      <slot name="activator" v-bind="slotProps"></slot>
    </template>
    <v-card elevation="2" class="flex-fill">
      <v-card-title>
        <v-col>Running Sessions</v-col>
        <v-col class="text-right"><v-icon @click="fetchSessions()">refresh</v-icon></v-col>
      </v-card-title>
      <v-card-text>
        <v-treeview
          rounded
          hoverable
          item-key="id"
          item-children="children"
          item-text="description"
          :items="sessions"
          :open="open"
        >
          <template v-slot:append="{ item }">
            <v-row>
              <v-col>
                <v-btn
                  @click.stop="useSession(item.id)"
                >Jump to Session</v-btn>
              </v-col>
              <v-col>
                <v-btn
                  @click.stop="closeSession(item.id)"
                >Close</v-btn>
              </v-col>
            </v-row>
          </template>
        </v-treeview>
        <project-selector>
          <template v-slot:activator="{ on, attrs }">
            <v-btn
              v-bind="attrs"
              v-on="on"
            >
              Open Project
            </v-btn>
          </template>
        </project-selector>
      </v-card-text>
    </v-card>
  </v-dialog>
</template>


<script>
import ProjectSelector from "@/components/ProjectSelector";

export default {
  name: 'SessionSelector',
  components: {
    ProjectSelector
  },

  data: () => ({
    sessions: [],
    show: false,
  }),

  computed: {
    open() {
      // Forcibly reopen all elements whenever items change
      return this.sessions.map(k => {return k.id})
    }
  },

  mounted() {
    this.fetchSessions()
  },

  methods: {
    useSession(session) {
      this.$api.setCurrentSession(session)
      this.show = false
    },
    closeSession(session) {
      this.$api.closeSession(session)
    },
    fetchSessions() {
      return this.$api.listSessions().then(response => {
        this.sessions = response.sessions.map(s => {
          return {
            id: s.id,
            description: "Project " + s.project
          }
        })
        this.show = true
      })
      .catch(error => {
        error
        return []
      })
    }
  }
};
</script>
