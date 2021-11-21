<template>
  <v-container fluid>
    <v-select
      v-model="selection"
      :items="targets"
      chips
      label="Filter by Build Target"
      multiple
      clearable
      deletable-chips
    ></v-select>
  </v-container>
</template>

<script>

export default {
  name: 'TargetSelector',

  props : {
    selection: [],
  },

  data: () => ({
      targets: [],
  }),

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.$api.getTargetCounts('target')
        .then(response => {
          this.targets =  Object.keys(response.data)
        })
    }
  }
};
</script>

<style>
</style>
