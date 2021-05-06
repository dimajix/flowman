<template>
  <v-container>
    <v-card elevation="2">
      <v-card-title>Running Kernels and Sessions</v-card-title>
      <v-card-text>
        <v-treeview
          open-all
          rounded
          hoverable
          :items="sessions"
        >
          <template v-slot:append="{ item }">
            <v-btn
              v-if="item.session"
            >Jump to Session {{item.id}}</v-btn>
            <v-btn
              v-if="!item.session"
            >Create New Session</v-btn>
          </template>
        </v-treeview>
        <v-btn>Start new Kernel</v-btn>
      </v-card-text>
    </v-card>
  </v-container>
</template>


<script>
export default {
  name: 'Kernels',
  components: {},
  data: () => ({
    sessions: [
      {
        id: 1,
        name: 'Kernel 1 :',
        children: [
          { id: 2, name: 'Session A', session:'abc' },
          { id: 3, name: 'Session B', session:'abc' }
        ],
      },
      {
        id: 4,
        name: 'Kernel 2 :',
        children: [
          { id: 5, name: 'Session X', session:'abc' },
          { id: 6, name: 'Session Y', session:'abc' }
        ],
      },
    ],
  }),
};
</script>
