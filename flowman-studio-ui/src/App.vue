<template>
  <v-app>
    <v-app-bar
      app color="primary"
      elevate-on-scroll
    >
      <v-toolbar-title
        class="flex-fill"
      >
        Flowman Studio: {{projectName}}
      </v-toolbar-title>
      <v-btn class="ma-2"
        @click.stop="resetSession()"
      >Reset Session</v-btn>
      <v-btn class="ma-2"
        @click.stop="reloadProject()"
      >Reload Project</v-btn>
      <session-selector>
        <template v-slot:activator="{ on, attrs }">
          <v-btn
            class="ma-2"
            v-bind="attrs"
            v-on="on"
          >
            Switch Session & Project
          </v-btn>
        </template>
      </session-selector>
    </v-app-bar>

    <v-main>
      <v-container class="fill-height, container--fluid">
        <v-row  class="fill-height">
          <v-col class="col-lg-2">
            <v-row>
               <navigation-menu
                @select-job="selectJob"
                @execute-job="executeJob"
                @select-test="selectTest"
                @execute-test="executeTest"
                @select-target="selectTarget"
                @execute-target="executeTarget"
                @select-mapping="selectMapping"
                @collect-mapping="collectMapping"
                @inspect-mapping-schema="inspectMappingSchema"
                @select-relation="selectRelation"
                @collect-relation="collectRelation"
                @inspect-relation-schema="inspectRelationSchema"
               ></navigation-menu>
            </v-row>
            <v-row>
              <job-properties v-if="properties === 'job'" :job="job"></job-properties>
              <test-properties v-if="properties === 'test'" :test="test"></test-properties>
              <target-properties v-if="properties === 'target'" :target="target"></target-properties>
              <mapping-properties v-if="properties === 'mapping'" :mapping="mapping"></mapping-properties>
              <relation-properties v-if="properties === 'relation'" :relation="relation"></relation-properties>
            </v-row>
          </v-col>
          <v-col>
              <v-row>
                <v-col>
                  <flow></flow>
                </v-col>
              </v-row>
              <v-divider/>
              <v-row>
                <v-col>
                  <v-sheet elevation="2">
                    <v-card-title>Data Inspector</v-card-title>
                    <v-tabs
                      v-model="tab"
                      next-icon="mdi-arrow-right-bold-box-outline"
                      prev-icon="mdi-arrow-left-bold-box-outline"
                      show-arrows
                    >
                      <v-tabs-slider color="yellow"></v-tabs-slider>
                      <v-tab
                        v-for="item in outputTabs"
                        :key="item.id"
                      >
                        <v-icon v-if="item.reload === true">refresh</v-icon>
                        {{ item.title }}
                        <v-icon v-if="item.close === true" @click="closeTab(item.id)">close</v-icon>
                      </v-tab>
                    </v-tabs>

                    <v-tabs-items  v-model="tab">
                      <v-tab-item
                        v-for="item in outputTabs"
                        :key="item.id"
                      >
                        <v-card
                          color="basil"
                          flat
                        >
                          <log-output v-if="item.kind === 'log'"></log-output>
                          <mapping-output v-if="item.kind === 'mapping'"></mapping-output>
                        </v-card>
                      </v-tab-item>
                    </v-tabs-items>
                  </v-sheet>
                </v-col>
              </v-row>
          </v-col>
        </v-row>
      </v-container>
    </v-main>

    <template>
      <v-footer class="pa-3" app>
        <v-spacer></v-spacer>
        <div>&copy; {{ new Date().getFullYear() }}</div>
      </v-footer>
    </template>
  </v-app>
</template>


<script>
import NavigationMenu from '@/components/NavigationMenu'
import LogOutput from '@/components/LogOutput'
import MappingOutput from '@/components/MappingOutput'
import JobProperties from '@/components/JobProperties'
import TestProperties from '@/components/TestProperties'
import TargetProperties from '@/components/TargetProperties'
import MappingProperties from '@/components/MappingProperties'
import RelationProperties from '@/components/RelationProperties'
import Flow from '@/components/Flow'
import SessionSelector from '@/components/SessionSelector'

export default {
  name: 'Workbench',
  components: {
    NavigationMenu,
    JobProperties,
    TestProperties,
    TargetProperties,
    MappingProperties,
    RelationProperties,
    LogOutput,
    MappingOutput,
    Flow,
    SessionSelector
  },

  data () {
    return {
      tab: null,
      outputTabs: [
        {id:"log", kind:"log", title:"Log Output", close:false, reload:false},
        {id:"mapping-1", kind:"mapping", title:"Mapping 1", close:true, reload:true},
        {id:"mapping-2", kind:"mapping", title:"Mapping 2", close:true, reload:true},
        {id:"mapping-3", kind:"mapping", title:"Mapping 3", close:true, reload:true}
      ],
      projectName: null,
      properties: null,
      job: null,
      test: null,
      target: null,
      mapping: null,
      relation: null,
    }
  },

  mounted() {
    this.refreshProject()
  },

  computed: {
    kernel: function() { return this.$api.state.kernel },
    session: function() { return this.$api.state.session }
  },

  watch: {
    kernel: function () { this.refreshProject() },
    session: function() { this.refreshProject() }
  },

  methods: {
    refreshProject() {
      this.$api.getCurrentSession()
        .then(s => { this.projectName = s.project })
      this.outputTabs = this.outputTabs.filter(t => t.kind === "log")
    },
    reloadProject() {

    },
    resetSession() {
      this.$api.resetSession(this.kernel, this.session)
    },
    closeTab(id) {
      this.outputTabs = this.outputTabs.filter(t => t.id !== id)
    },
    selectJob(job) {
      this.properties = "job"
      this.job = job
    },
    executeJob(job) {
      //console.log("Execute job", job)
      job
    },
    selectTest(test) {
      this.properties = "test"
      this.test = test
    },
    executeTest(test) {
      //console.log("Execute test", test)
      test
    },
    selectTarget(target) {
      this.properties = "target"
      this.target = target
    },
    executeTarget(target) {
      //console.log("Execute target", target)
      target
    },
    selectMapping(mapping) {
      this.properties = "mapping"
      this.mapping = mapping
    },
    collectMapping(mapping) {
      //console.log("Collect mapping", mapping)
      mapping
    },
    inspectMappingSchema(mapping) {
      //console.log("Inspect  mapping schema", mapping)
      mapping
    },
    selectRelation(relation) {
      this.properties = "relation"
      this.relation = relation
    },
    collectRelation(relation) {
      //console.log("Collect relation", relation)
      relation
    },
    inspectRelationSchema(relation) {
      //console.log("Inspect  relation schema", relation)
      relation
    },
  }
};
</script>
