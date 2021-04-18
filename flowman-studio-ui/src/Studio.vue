<template>
  <v-app>
    <v-app-bar app color="primary">
      <v-app-bar-title>Flowman Studio</v-app-bar-title>
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
                    <v-tabs
                      v-model="tab"
                      grow="true"
                      next-icon="mdi-arrow-right-bold-box-outline"
                      prev-icon="mdi-arrow-left-bold-box-outline"
                      show-arrows
                    >
                      <v-tabs-slider color="yellow"></v-tabs-slider>
                      <v-tab
                        v-for="i in 3"
                        :key="i"
                      >
                        Mapping {{ i }}
                      </v-tab>
                    </v-tabs>

                    <v-tabs-items  v-model="tab">
                      <v-tab-item
                        v-for="i in 3"
                        :key="i"
                      >
                        <v-card
                          color="basil"
                          flat
                        >
                          <mapping-output></mapping-output>
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
import NavigationMenu from './components/NavigationMenu'
import MappingOutput from './components/MappingOutput'
import MappingProperties from './components/MappingProperties'
import Flow from './components/Flow'

export default {
  name: 'App',
  components: {
      NavigationMenu,
      MappingProperties,
      MappingOutput,
      Flow
  },
  data () {
    return {
      tab: null
    }
  },
};
</script>
