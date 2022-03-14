<template>
    <fieldset class="checkbox__group" :id="id">
        <legend>{{ title }}</legend>
        <div
            class="checkbox__item"
            v-for="choice in choices"
            v-bind:key="choice.id"
        >
            <input
                type="checkbox"
                :id="choice.id"
                v-bind:value="choice.value"
                v-bind:name="name"
                @change="updateCheckbox($event.target)"
                v-bind:checked="choice.checked"
            />
            <label v-bind:for="choice.id">{{ choice.label }}</label>
        </div>
    </fieldset>
</template>

<script>
export default {
  props: {
    id: String,
    name: String,
    choices: Object,
    title: String
  },
  methods: {
    updateCheckbox (target) {
      return this.$emit('change', {
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
.checkbox__group {
    .checkbox__item {
        padding: 0.65em 1em;
        background-color: #ffffff;
        border-bottom: 1px solid #f6f6f6;

        input,
        label {
            cursor: pointer;
        }
    }
}
</style>
