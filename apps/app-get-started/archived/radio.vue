<template>
    <div class="radioitem">
        <input
            type="radio"
            :id="id"
            :name="name"
            :value="value"
            @change="updateRadio($event.target)"
            :checked="ischecked"
        />
        <label :for="id">{{ title }}</label>
    </div>
</template>

<script>
export default {
  props: {
    id: {
      type: String,
      required: true
    },
    name: {
      type: String,
      required: true
    },
    value: {
      type: String,
      required: true
    },
    ischecked: {
      type: String,
      default: 'false',
      required: false
    },
    title: {
      type: String,
      required: true
    }
  },
  methods: {
    updateRadio (target) {
      this.$emit('change', {
        id: target.id,
        name: target.name,
        value: target.value,
        checked: target.checked
      })
    }
  }
}
</script>

<style lang="scss">
.radiogroup {

    legend {
        color: #252525;
        font-weight: bold;
        margin-bottom: 12px;
    }

    .radioitem {
        padding: 0.65em 1em;
        background-color: #ffffff;
        border-bottom: 1px solid #f6f6f6;

        input, label {
            cursor: pointer;
        }
    }
}
</style>
