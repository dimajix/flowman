<template>
  <v-chip
    :small="small"
    :color="getColor(phase)"
  >
    <v-icon>{{ getIcon(phase) }}</v-icon>&nbsp; {{ phase }}
  </v-chip>

</template>


<script>
export default {
  name: 'Phase',

  props: {
    phase: { type: String },
    small: {
      type: Boolean,
      default: () => {
        return false
      }
    }
  },

  methods: {
    getColor(phase) {
      if (phase === "VALIDATE") {
        return "#d0d752"
      }
      else if (phase === "CREATE") {
        return "#96be4f"
      }
      else if (phase === "BUILD") {
        return "#41B883"
      }
      else if (phase === "VERIFY") {
        return "#00D8FF"
      }
      else if (phase === "TRUNCATE") {
        return "#b45b93"
      }
      else if (phase === "DESTROY") {
        return "#E42651"
      }
      else {
        return "#809080"
      }
    },

    getIcon(phase) {
      if (phase === "VERIFY") {
        return "done"
      }
      else if (phase === "CREATE") {
        return "add"
      }
      else if (phase === "BUILD") {
        return "sync"
      }
      else if (phase === "VALIDATE") {
        return "done_all"
      }
      else if (phase === "TRUNCATE") {
        return "clear"
      }
      else if (phase === "DESTROY") {
        return "delete"
      }
      else {
        return "warning_amber"
      }
    }
  },

};
</script>
