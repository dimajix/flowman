<template>
  <v-virtual-scroll
    :items="lines"
    height="400"
    item-height="32"
    >
    <template v-slot:default="{ item }">
      <v-list-item>
        <v-list-item-content>
          {{item.timestamp}}
          {{item.message}}
        </v-list-item-content>
      </v-list-item>
    </template>
  </v-virtual-scroll>
</template>

<script>
export default {
  name: 'LogOutput',

  mounted() {
  },

  data() {
    return {
      lines: [
      ],
    }
  },

  computed: {
    kernel: function() { return this.$api.state.kernel }
  },

  watch: {
    kernel: function () {
      this.lines = []
      this.setupStream()
    }
  },

  methods: {
    setupStream() {
      this.$api.getKernelLog()
        .on('message', (msg) => {
          this.lines.push(msg)
        })
        .connect()
        //.catch((err) => console.error('Failed make initial connection:', err));
    }
  },
}
</script>
