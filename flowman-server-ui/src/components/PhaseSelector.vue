<template>
  <v-container fluid>
    <v-row align="center">
      <v-col cols="2">
        <v-subheader>Phases</v-subheader>
      </v-col>
      <v-col cols="9">
        <v-select
          v-model="value"
          :items="phases"
          chips
          label="Filter by Build Phase"
          multiple
          clearable
          deletable-chips
          @input='$emit("input", value)'
        ></v-select>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>

export default {
  name: 'PhaseSelector',

  data() {
    return {
      value: [],
      phases: ["VALIDATE", "CREATE", "BUILD", "VERIFY", "TRUNCATE", "DESTROY"],
    };
  },

  mounted() {
  },
};
</script>

<style>
</style>
