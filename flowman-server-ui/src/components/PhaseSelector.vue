<template>
  <v-container fluid>
    <v-select
      v-model="selection"
      :items="phases"
      chips
      label="Filter by Build Phase"
      multiple
      clearable
      deletable-chips
    ></v-select>
  </v-container>
</template>

<script>

export default {
  name: 'PhaseSelector',

  props : {
    selection: [],
  },

  data() {
    return {
      phases: ["VALIDATE", "CREATE", "BUILD", "VERIFY", "TRUNCATE", "DESTROY"],
    };
  },

  mounted() {
  },
};
</script>

<style>
</style>
