<template>
  <v-container>
    <v-card>
    <v-list nav dense>
      <v-list-item link>
        <v-list-item-action>
          <v-icon>info</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          System
        </v-list-item-title>
      </v-list-item>

      <v-list-item link>
        <v-list-item-action>
          <v-icon>domain</v-icon>
        </v-list-item-action>
        <v-list-item-title>
          Namespace
        </v-list-item-title>
      </v-list-item>

      <v-list-item link>
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
              <v-icon>gavel</v-icon>
            </v-list-item-action>
            <v-list-item-title>Jobs</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in jobs"
          :key="item"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>check</v-icon>
            </v-list-item-action>
            <v-list-item-title>Tests</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in tests"
          :key="item"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>label</v-icon>
            </v-list-item-action>
            <v-list-item-title>Targets</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in targets"
          :key="item"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>widgets</v-icon>
            </v-list-item-action>
            <v-list-item-title>Mappings</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in mappings"
          :key="item"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>table</v-icon>
            </v-list-item-action>
            <v-list-item-title>Relations</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in relations"
          :key="item"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
        </v-list-item>
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
      jobs: [],
      targets: [],
      tests: [],
      mappings: [],
      relations: []
    }
  },

  mounted() {
    this.refreshProject()
  },

  computed: {
    session: function() { return this.$api.state.session }
  },

  watch: {
    session: function() { this.refreshProject() }
  },

  methods: {
    refreshProject() {
      this.$api.listJobs().then(s => { this.jobs = s.jobs.sort() })
      this.$api.listTargets().then(s => { this.targets = s.targets.sort() })
      this.$api.listTests().then(s => { this.tests = s.tests.sort() })
      this.$api.listMappings().then(s => { this.mappings = s.mappings.sort() })
      this.$api.listRelations().then(s => { this.relations = s.relations.sort() })
    },
  }
}
</script>
