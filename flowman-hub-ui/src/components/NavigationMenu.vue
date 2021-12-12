<template>
  <v-container>
    <v-card>
    <v-list nav dense>
      <v-list-item link>
        <v-list-item-action><v-icon>info</v-icon></v-list-item-action>
        <v-list-item-title>System</v-list-item-title>
      </v-list-item>

      <v-list-item link>
        <v-list-item-action><v-icon>domain</v-icon></v-list-item-action>
        <v-list-item-title>Namespace</v-list-item-title>
      </v-list-item>

      <v-list-item link>
        <v-list-item-action><v-icon>account_tree</v-icon></v-list-item-action>
        <v-list-item-title>Project</v-list-item-title>
      </v-list-item>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action><v-icon>gavel</v-icon></v-list-item-action>
            <v-list-item-title>Jobs</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in jobs"
          :key="item"
          @click.stop="clickJob(item)"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
          <v-list-item-action>
            <v-tooltip bottom>
              <template v-slot:activator="{ on, attrs }">
                <v-btn
                  v-bind="attrs"
                  @click.stop="executeJob(item)"
                  icon><v-icon>play_arrow</v-icon>
                </v-btn>
              </template>
              <span>Execute job</span>
            </v-tooltip>
          </v-list-item-action>
        </v-list-item>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action><v-icon>rule</v-icon></v-list-item-action>
            <v-list-item-title>Tests</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in tests"
          :key="item"
          @click.stop="clickTest(item)"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
          <v-list-item-action>
            <v-tooltip bottom>
              <template v-slot:activator="{ on, attrs }">
                <v-btn
                  v-bind="attrs"
                  @click.stop="executeTest(item)"
                  icon><v-icon>play_arrow</v-icon>
                </v-btn>
              </template>
              <span>Execute test</span>
            </v-tooltip>
          </v-list-item-action>
        </v-list-item>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action><v-icon>label</v-icon></v-list-item-action>
            <v-list-item-title>Targets</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in targets"
          :key="item"
          @click.stop="clickTarget(item)"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
          <v-list-item-action>
            <v-tooltip bottom>
              <template v-slot:activator="{ on, attrs }">
                <v-btn
                  v-bind="attrs"
                  @click.stop="executeTarget(item)"
                  icon><v-icon>play_arrow</v-icon>
                </v-btn>
              </template>
              <span>Execute target</span>
            </v-tooltip>
          </v-list-item-action>
        </v-list-item>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action><v-icon>mediation</v-icon></v-list-item-action>
            <v-list-item-title>Mappings</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in mappings"
          :key="item"
          @click.stop="clickMapping(item)"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
          <v-list-item-action>
            <v-tooltip bottom>
              <template v-slot:activator="{ on, attrs }">
                <v-btn
                  v-bind="attrs"
                  @click.stop="collectMapping(item)"
                  icon><v-icon>manage_search</v-icon>
                </v-btn>
              </template>
              <span>View records</span>
            </v-tooltip>
          </v-list-item-action>
          <v-list-item-action>
            <v-tooltip bottom>
              <template v-slot:activator="{ on, attrs }">
                <v-btn
                  v-bind="attrs"
                  @click.stop="inspectMappingSchema(item)"
                  icon><v-icon>schema</v-icon>
                </v-btn>
              </template>
              <span>Inspect schema</span>
            </v-tooltip>
          </v-list-item-action>
        </v-list-item>
      </v-list-group>

      <v-list-group no-action>
        <template v-slot:activator>
          <v-list-item>
            <v-list-item-action>
              <v-icon>table_view</v-icon>
            </v-list-item-action>
            <v-list-item-title>Relations</v-list-item-title>
          </v-list-item>
        </template>
        <v-list-item
          v-for="item in relations"
          :key="item"
          @click.stop="clickRelation(item)"
          link
        >
          <v-list-item-content>
            <v-list-item-title>{{ item }}</v-list-item-title>
          </v-list-item-content>
          <v-list-item-action>
            <v-tooltip bottom>
              <template v-slot:activator="{ on, attrs }">
                <v-btn
                  v-bind="attrs"
                  @click.stop="collectRelation(item)"
                  icon><v-icon>manage_search</v-icon>
                </v-btn>
              </template>
              <span>View records</span>
            </v-tooltip>
          </v-list-item-action>
          <v-list-item-action>
            <v-tooltip bottom>
              <template v-slot:activator="{ on, attrs }">
                <v-btn
                  v-bind="attrs"
                  @click.stop="inspectRelationSchema(item)"
                  icon><v-icon>schema</v-icon>
                </v-btn>
              </template>
              <span>Inspect schema</span>
            </v-tooltip>
          </v-list-item-action>
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
    clickJob(job) { this.$emit('select-job', job) },
    executeJob(job) { this.$emit('execute-job', job) },
    clickTest(test) { this.$emit('select-test', test) },
    executeTest(test) { this.$emit('execute-test', test) },
    clickTarget(target) { this.$emit('select-target', target) },
    executeTarget(target) { this.$emit('execute-target', target) },
    clickMapping(mapping) { this.$emit('select-mapping', mapping) },
    collectMapping(mapping) { this.$emit('collect-mapping', mapping) },
    inspectMappingSchema(mapping) { this.$emit('inspect-mapping-schema', mapping) },
    clickRelation(relation) { this.$emit('select-relation', relation) },
    collectRelation(relation) { this.$emit('collect-relation', relation) },
    inspectRelationSchema(relation) { this.$emit('inspect-relation-schema', relation) },
  }
}
</script>
