import { GET_DATA } from "../Action";

const initialState = {
  data: null,
  paginations: null,
}

const ApiReducer = (state = initialState, action) => {
  switch (action.type) {
    case GET_DATA:
      return {
        ...state,
        data: action.data,
        paginations: action.paginations,
      }
  }
  return state
}

export default ApiReducer
