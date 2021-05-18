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
      <sessions>
        <template v-slot:activator="{ on, attrs }">
          <v-btn
            class="ma-2"
            v-bind="attrs"
            v-on="on"
          >
            Switch Kernel & Project
          </v-btn>
        </template>
      </sessions>
    </v-app-bar>

    <v-main>
      <v-container class="fill-height, container--fluid">
        <v-row  class="fill-height">
          <v-col class="col-lg-2">
            <v-row>
               <navigation-menu></navigation-menu>
            </v-row>
            <v-row>
                <mapping-properties></mapping-properties>
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
import MappingProperties from '@/components/MappingProperties'
import Flow from '@/components/Flow'
import Sessions from '@/components/Sessions'

export default {
  name: 'Workbench',
  components: {
      NavigationMenu,
      MappingProperties,
      LogOutput,
      MappingOutput,
      Flow,
      Sessions
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
      projectName: null
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
    }
  }
};
</script>
